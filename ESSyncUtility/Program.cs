using Microsoft.Extensions.Configuration;
using Serilog;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Elastic.Transport.Products.Elasticsearch;

namespace ESSyncUtility
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
            var connectionString = configuration.GetConnectionString("DefaultConnection");
            var sqlQuery = configuration["Database:SqlQuery"];
            var updateFields = configuration.GetSection("UpdateFields").Get<string[]>();
            var elasticsearchUrl = configuration["Elasticsearch:Url"];
            var elasticsearchIndexes = configuration.GetSection("Elasticsearch:Indexes").Get<List<string>>();
            var elasticsearchUsername = configuration["Elasticsearch:Username"];
            var elasticsearchPassword = configuration["Elasticsearch:Password"];
            var disableCertificateValidation = bool.Parse(configuration["Elasticsearch:DisableCertificateValidation"] ?? "false");
            var logLevel = configuration["Logging:LogLevel"];
            var logFilePath = configuration["Logging:LogFilePath"];
            var batchSize = int.Parse(configuration["BatchSize"] ?? "100");

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
                ValidateConfigurations(connectionString, sqlQuery, updateFields, elasticsearchUsername, elasticsearchPassword, elasticsearchIndexes);

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

                var fields = GetFieldsFromMapping(mappingResponse, indexName);

                if (fields == null || fields.Length == 0)
                {
                    throw new Exception("No fields found in index mappings.");
                }

                Log.Information("Fields fetched from index mappings: {Fields}", string.Join(", ", fields));

                var orderService = new OrderService(connectionString, sqlQuery, batchSize);
                var esService = new ElasticsearchService(
                    elasticsearchUrl,
                    elasticsearchIndexes,
                    elasticsearchUsername,
                    elasticsearchPassword,
                    disableCertificateValidation
                );
                var fieldSelector = DynamicFieldMapper.CreateFieldSelector(fields);

                int offset = 0;
                int totalProcessed = 0;

                while (true)
                {
                    var orders = await orderService.GetOrdersAsync(offset);

                    if (orders == null || !orders.Any())
                    {
                        break;
                    }

                    var documents = new List<Dictionary<string, object>>();

                    foreach (var order in orders)
                    {
                        var selectedFields = fieldSelector(order);
                        documents.Add(selectedFields);
                    }

                    await esService.BulkPartialUpdateAsync(documents, updateFields);

                    totalProcessed += documents.Count;
                    offset += documents.Count;

                    Log.Information("{TotalProcessed} records processed.", totalProcessed);
                    Console.WriteLine($"{totalProcessed} records processed.");
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
                var keyName = kvp.Key.Name;

                var fieldName = parentField != null ? $"{parentField}.{keyName}" : keyName;

                fieldNames.Add(fieldName);

                if (kvp.Value is ObjectProperty objectProperty && objectProperty.Properties != null)
                {
                    fieldNames.AddRange(GetFieldNames(objectProperty.Properties, fieldName));
                }
            }

            return fieldNames;
        }

        static void ValidateConfigurations(
            string connectionString,
            string sqlQuery,
            string[] updateFields,
            string elasticsearchUsername,
            string elasticsearchPassword,
            List<string> elasticsearchIndexes)
        {
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentException("Connection string cannot be null or empty.");
            }

            if (string.IsNullOrEmpty(sqlQuery))
            {
                throw new ArgumentException("SQL query cannot be null or empty.");
            }

            if (updateFields == null || updateFields.Length == 0)
            {
                throw new ArgumentException("UpdateFields array cannot be null or empty.");
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