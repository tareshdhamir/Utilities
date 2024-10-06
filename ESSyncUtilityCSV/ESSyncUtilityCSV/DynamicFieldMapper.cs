using System;
using System.Collections.Generic;
using Serilog;

namespace ESSyncUtilityCSV
{
    public static class DynamicFieldMapper
    {
        public static Func<Dictionary<string, object>, Dictionary<string, object>> CreateFieldSelector(string[] fields)
        {
            return (Dictionary<string, object> obj) =>
            {
                var dict = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

                foreach (var field in fields)
                {
                    if (obj.TryGetValue(field, out var value))
                    {
                        dict.Add(field, value);
                    }
                    else
                    {
                        // Handle missing field
                        Log.Warning("Field '{Field}' not found in data source.", field);
                    }
                }

                return dict;
            };
        }
    }
}
