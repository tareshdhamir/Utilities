using System;
using System.Collections.Generic;
using System.Dynamic;
using Serilog;


namespace ESSyncUtility
{
    public static class DynamicFieldMapper
{
    public static Func<dynamic, Dictionary<string, object>> CreateFieldSelector(string[] fields)
    {
        return (dynamic obj) =>
        {
            var dict = new Dictionary<string, object>();
            var objDict = obj as IDictionary<string, object>;

            if (objDict == null)
            {
                objDict = new ExpandoObject();
                foreach (var property in obj.GetType().GetProperties())
                {
                    objDict.Add(property.Name, property.GetValue(obj));
                }
            }

            foreach (var field in fields)
            {
                if (objDict.TryGetValue(field, out var value))
                {
                    dict.Add(field, value);
                }
                else
                {
                    // Handle missing field
                    Log.Warning($"Field '{field}' not found in data source.");
                }
            }

            return dict;
        };
    }
}
}