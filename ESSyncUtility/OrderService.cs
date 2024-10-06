using Dapper;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System;
using System.Data;
using System.Linq;

namespace ESSyncUtility
{
    public class OrderService
    {
        private readonly string _connectionString;
        private readonly string _sqlQuery;
        private readonly int _batchSize;

        public OrderService(string connectionString, string sqlQuery, int batchSize)
        {
            _connectionString = connectionString;
            _sqlQuery = sqlQuery;
            _batchSize = batchSize;
        }

        public async Task<IEnumerable<dynamic>> GetOrdersAsync(int offset)
        {
            using var connection = new SqlConnection(_connectionString);
            try
            {
                var paginatedQuery = $"{_sqlQuery} ORDER BY OrderId OFFSET {offset} ROWS FETCH NEXT {_batchSize} ROWS ONLY";
                var orders = await connection.QueryAsync(paginatedQuery);
                return orders;
            }
            catch (Exception ex)
            {
                throw new Exception("Error fetching data from database.", ex);
            }
        }
    }

}