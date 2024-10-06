using CsvHelper;
using CsvHelper.Configuration;
using System.Globalization;

namespace ESSyncUtilityCSV
{
    public class CsvDataService
    {
        private readonly string _directoryPath;
        private readonly int _batchSize;

        public CsvDataService(string directoryPath, int batchSize)
        {
            _directoryPath = directoryPath;
            _batchSize = batchSize;
        }

        public IEnumerable<string> GetCsvFiles()
        {
            return Directory.EnumerateFiles(_directoryPath, "*.csv");
        }

        public string[] GetCsvHeaders(string csvFilePath)
        {
            using var reader = new StreamReader(csvFilePath);
            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                MissingFieldFound = null,
                HeaderValidated = null
            };

            using var csv = new CsvReader(reader, config);

            csv.Read();
            csv.ReadHeader();
            return csv.HeaderRecord;
        }

        public async IAsyncEnumerable<List<Dictionary<string, object>>> ReadCsvDataAsync(string csvFilePath)
        {
            using var reader = new StreamReader(csvFilePath);
            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                MissingFieldFound = null,
                HeaderValidated = null
            };

            using var csv = new CsvReader(reader, config);

            var records = new List<Dictionary<string, object>>();

            await csv.ReadAsync();
            csv.ReadHeader();

            while (await csv.ReadAsync())
            {
                var record = csv.GetRecord<dynamic>() as IDictionary<string, object>;

                if (record != null)
                {
                    records.Add(new Dictionary<string, object>(record, StringComparer.OrdinalIgnoreCase));
                }

                if (records.Count >= _batchSize)
                {
                    yield return records;
                    records = new List<Dictionary<string, object>>();
                }
            }

            if (records.Any())
            {
                yield return records;
            }
        }
    }
}
