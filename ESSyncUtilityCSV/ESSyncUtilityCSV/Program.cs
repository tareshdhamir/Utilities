using Microsoft.Extensions.Configuration;
using Serilog;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Transport.Products.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;

namespace ESSyncUtilityCSV
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // Build configuration
            var configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json")
                .Build();

            // Read configurations
            var csvDirectory = configuration["CSV:Directory"];
            var elasticsearchUrl = configuration["Elasticsearch:Url"];
            var elasticsearchIndexes = configuration.GetSection("Elasticsearch:Indexes").Get<List<string>>();
            var elasticsearchUsername = configuration["Elasticsearch:Username"];
            var elasticsearchPassword = configuration["Elasticsearch:Password"];
            var disableCertificateValidation = bool.Parse(configuration["Elasticsearch:DisableCertificateValidation"] ?? "false");
            var logLevel = configuration["Logging:LogLevel"];
            var logFilePath = configuration["Logging:LogFilePath"];
            var batchSize = int.Parse(configuration["BatchSize"] ?? "1000"); // Adjust batch size for performance

            // Initialize logging
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console()
                .WriteTo.File(logFilePath, rollingInterval: RollingInterval.Day)
                .MinimumLevel.Is(Enum.Parse<Serilog.Events.LogEventLevel>(logLevel))
                .CreateLogger();

            try
            {
                Log.Information("Application Started");
                Console.WriteLine("Application Started");

                // Validate configurations
                ValidateConfigurations(csvDirectory, elasticsearchUsername, elasticsearchPassword, elasticsearchIndexes);

                // Initialize Elasticsearch client to fetch fields
                var esClientSettings = new ElasticsearchClientSettings(new Uri(elasticsearchUrl));

                if (!string.IsNullOrEmpty(elasticsearchUsername) && !string.IsNullOrEmpty(elasticsearchPassword))
                {
                    esClientSettings = esClientSettings.Authentication(new BasicAuthentication(elasticsearchUsername, elasticsearchPassword));
                }

                if (disableCertificateValidation)
                {
                    esClientSettings = esClientSettings.ServerCertificateValidationCallback(CertificateValidations.AllowAll);
                }

                var esClient = new ElasticsearchClient(esClientSettings);

                // Fetch fields from the first index
                var indexName = elasticsearchIndexes.First();
                var mappingResponse = await esClient.Indices.GetMappingAsync(new GetMappingRequest(indexName));

                if (!mappingResponse.IsSuccess())
                {
                    if (mappingResponse.TryGetElasticsearchServerError(out var serverError))
                    {
                        throw new Exception($"Failed to get index mappings: {serverError.Error.Reason}");
                    }
                    else if (mappingResponse.TryGetOriginalException(out var originalException))
                    {
                        throw new Exception($"Failed to get index mappings: {originalException.Message}", originalException);
                    }
                    else
                    {
                        throw new Exception("Failed to get index mappings: Unknown error.");
                    }
                }

                var indexFields = GetFieldsFromMapping(mappingResponse, indexName);

                if (indexFields == null || indexFields.Length == 0)
                {
                    throw new Exception("No fields found in index mappings.");
                }

                Log.Information("Fields fetched from index mappings: {Fields}", string.Join(", ", indexFields));

                var esService = new ElasticsearchService(
                    elasticsearchUrl,
                    elasticsearchIndexes,
                    elasticsearchUsername,
                    elasticsearchPassword,
                    disableCertificateValidation
                );

                var csvDataService = new CsvDataService(csvDirectory, batchSize);

                var processedRecords = new HashSet<string>(); // To keep track of processed records

                foreach (var csvFile in csvDataService.GetCsvFiles())
                {
                    Log.Information("Processing CSV file: {FileName}", csvFile);
                    Console.WriteLine($"Processing CSV file: {csvFile}");

                    try
                    {
                        // Read CSV header to get update fields
                        var updateFields = csvDataService.GetCsvHeaders(csvFile);

                        // Validate headers against index fields
                        ValidateCsvHeaders(updateFields, indexFields);

                        Log.Information("CSV headers validated.");

                        // Initialize field selector
                        var fieldSelector = DynamicFieldMapper.CreateFieldSelector(updateFields);

                        // Process CSV data
                        await foreach (var records in csvDataService.ReadCsvDataAsync(csvFile))
                        {
                            // Filter out already processed records
                            var newRecords = records.Where(r => !processedRecords.Contains(r["OrderId"].ToString())).ToList();

                            if (newRecords.Count == 0)
                            {
                                continue;
                            }

                            var documents = new List<Dictionary<string, object>>();

                            foreach (var record in newRecords)
                            {
                                var selectedFields = fieldSelector(record);
                                documents.Add(selectedFields);
                            }

                            await esService.BulkPartialUpdateAsync(documents, updateFields);

                            // Mark records as processed
                            foreach (var record in newRecords)
                            {
                                processedRecords.Add(record["OrderId"].ToString());
                            }

                            Log.Information("{Count} records processed from {FileName}.", newRecords.Count, csvFile);
                            Console.WriteLine($"{newRecords.Count} records processed from {csvFile}.");
                        }

                        Log.Information("Finished processing CSV file: {FileName}", csvFile);
                        Console.WriteLine($"Finished processing CSV file: {csvFile}");
                    }
                    catch (Exception ex)
                    {
                        Log.Error(ex, "Error processing CSV file: {FileName}", csvFile);
                        Console.WriteLine($"Error processing CSV file: {csvFile}. Error: {ex.Message}");
                    }
                }

                Log.Information("Data synchronization completed.");
                Console.WriteLine("Data synchronization completed.");
            }
            catch (Exception ex)
            {
                Log.Error(ex, "An error occurred during processing.");
                Console.WriteLine($"An error occurred: {ex.Message}");
            }
            finally
            {
                Log.CloseAndFlush();
            }
        }

        static string[] GetFieldsFromMapping(GetMappingResponse mappingResponse, string indexName)
        {
            var indexEntry = mappingResponse.Indices
                .FirstOrDefault(kv => kv.Key.ToString().Equals(indexName, StringComparison.OrdinalIgnoreCase));

            if (indexEntry.Equals(default(KeyValuePair<IndexName, IndexState>)))
            {
                throw new Exception($"Index '{indexName}' not found in mappings.");
            }

            var indexMapping = indexEntry.Value;

            var properties = indexMapping.Mappings.Properties;

            var fields = GetFieldNames(properties);

            return fields.ToArray();
        }

        static List<string> GetFieldNames(Properties properties, string parentField = null)
        {
            var fieldNames = new List<string>();

            foreach (var kvp in properties)
            {
                var keyName = kvp.Key.ToString();

                var fieldName = parentField != null ? $"{parentField}.{keyName}" : keyName;

                fieldNames.Add(fieldName);

                if (kvp.Value is ObjectProperty objectProperty && objectProperty.Properties != null)
                {
                    fieldNames.AddRange(GetFieldNames(objectProperty.Properties, fieldName));
                }
            }

            return fieldNames;
        }

        static void ValidateCsvHeaders(string[] csvHeaders, string[] indexFields)
        {
            var missingFields = csvHeaders.Except(indexFields, StringComparer.OrdinalIgnoreCase).ToList();

            if (missingFields.Any())
            {
                throw new Exception($"CSV headers contain fields not present in the index mappings: {string.Join(", ", missingFields)}");
            }
        }

        static void ValidateConfigurations(
            string csvDirectory,
            string elasticsearchUsername,
            string elasticsearchPassword,
            List<string> elasticsearchIndexes)
        {
            if (string.IsNullOrEmpty(csvDirectory) || !Directory.Exists(csvDirectory))
            {
                throw new ArgumentException("CSV directory cannot be null, empty, or non-existent.");
            }

            if ((string.IsNullOrEmpty(elasticsearchUsername) && !string.IsNullOrEmpty(elasticsearchPassword)) ||
                (!string.IsNullOrEmpty(elasticsearchUsername) && string.IsNullOrEmpty(elasticsearchPassword)))
            {
                throw new ArgumentException("Both Elasticsearch username and password must be provided or omitted together.");
            }

            if (elasticsearchIndexes == null || !elasticsearchIndexes.Any())
            {
                throw new ArgumentException("Elasticsearch indexes list cannot be null or empty.");
            }
        }
    }
}
