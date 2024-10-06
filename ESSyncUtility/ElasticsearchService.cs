using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Serilog;

namespace ESSyncUtility
{
    public class ElasticsearchService
    {
        private readonly ElasticsearchClient _client;
        private readonly List<string> _indexes;

        public ElasticsearchService(string url, List<string> indexes, string username, string password, bool disableCertificateValidation)
        {
            var uri = new Uri(url);

            var settings = new ElasticsearchClientSettings(uri);

            if (!string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password))
            {
                settings = settings.Authentication(new BasicAuthentication(username, password));
            }

            if (disableCertificateValidation)
            {
                settings = settings.ServerCertificateValidationCallback(CertificateValidations.AllowAll);
            }

            _client = new ElasticsearchClient(settings);
            _indexes = indexes;
        }

        public async Task BulkPartialUpdateAsync(IEnumerable<Dictionary<string, object>> documents, string[] updateFields)
        {
            var documentsToUpdate = new List<(string Id, Dictionary<string, object> PartialDoc)>();

            foreach (var doc in documents)
            {
                if (doc.TryGetValue("OrderId", out var idObj))
                {
                    var id = idObj.ToString();

                    var partialDoc = new Dictionary<string, object>();
                    foreach (var field in updateFields)
                    {
                        if (doc.ContainsKey(field))
                        {
                            partialDoc[field] = doc[field];
                        }
                    }

                    documentsToUpdate.Add((Id: id, PartialDoc: partialDoc));
                }
                else
                {
                    Log.Warning("Document missing 'OrderId' field.");
                }
            }

            foreach (var index in _indexes)
            {
                var bulkDescriptor = new BulkRequestDescriptor(index);

                foreach (var docToUpdate in documentsToUpdate)
                {
                    bulkDescriptor.Update<dynamic>(u => u
                        .Id(docToUpdate.Id)
                        .Doc(docToUpdate.PartialDoc)
                        .DocAsUpsert(false)
                    );
                }

                try
                {
                    var response = await _client.BulkAsync(bulkDescriptor);

                    Log.Debug("Elasticsearch Bulk Response for index {Index}: {@Response}", index, response);

                    if (response.Errors)
                    {
                        foreach (var item in response.ItemsWithErrors)
                        {
                            Log.Error("Error updating index {Index}, document ID {Id}: {Error}", index, item.Id, item.Error);
                            Console.WriteLine($"Error updating index {index}, document ID {item.Id}: {item.Error.Reason}");
                        }
                    }
                    else
                    {
                        Log.Information("Bulk partial update successful for index {Index}.", index);
                        Console.WriteLine($"Bulk partial update successful for index {index}.");
                    }
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Error performing bulk partial update for index {Index}.", index);
                    Console.WriteLine($"Error performing bulk partial update for index {index}: {ex.Message}");
                    throw;
                }
            }
        }
    }


}