using System.Reflection;

namespace Polars.NET.Core.Helpers; 

public static class StructPacker
{
    public static SeriesHandle Pack<T>(string name, T[] rows)
    {
        Type type = typeof(T);
        PropertyInfo[] props = type.GetProperties();
        
        var fieldHandles = new List<SeriesHandle>();

        try 
        {
            foreach (var prop in props)
            {
                // Pivot (Row -> Col)
                Array columnData = ExtractColumnData(rows, prop);
                
                // Generate Series Handle
                SeriesHandle h = SeriesFactory.Create(prop.Name, columnData);
                fieldHandles.Add(h);
            }

            // Construct Struct Series
            return PolarsWrapper.SeriesNewStruct(name, fieldHandles.ToArray());
        }
        finally
        {
            // Dispose Handles
            foreach (var h in fieldHandles) h.Dispose();
        }
    }

    // Row[] -> Col[]
    private static Array ExtractColumnData<T>(T[] rows, PropertyInfo prop)
    {
        var count = rows.Length;
        Array arr = Array.CreateInstance(prop.PropertyType, count);
        for (int i = 0; i < count; i++)
        {
            arr.SetValue(prop.GetValue(rows[i]), i);
        }
        return arr;
    }
}
