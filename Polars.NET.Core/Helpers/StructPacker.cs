using System.Reflection;
using Polars.NET.Core.Arrow;

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
                SeriesHandle h = SeriesFactory.FromGenericArray(prop.Name, columnData);
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

internal static class SeriesFactory
{
    // --- Helpers to reduce boilerplate code ---

    // T?[] -> (T[], byte[]) -> Handle
    private static SeriesHandle NewNullable<T, U>(
        string name, 
        T?[] data, 
        Func<T?[], (U[] values, byte[]? mask)> unzipFunc,
        Func<string, U[], byte[]?, SeriesHandle> factory)
        where T : struct 
    {
        var (vals, mask) = unzipFunc(data);
        return factory(name, vals, mask);
    }

    // Temporal T[] -> U[] -> Handle
    private static SeriesHandle WithHelper<T, U>(
        string name, 
        T[] data, 
        Func<T[], U[]> convFunc,
        Func<string, U[], byte[], SeriesHandle> wrapperFunc)
    {
        var vals = convFunc(data);
        return wrapperFunc(name, vals, null!);
    }

    // Temporal Nullable : T?[] -> (U[], byte[]) -> Handle
    private static SeriesHandle WithHelperNullable<T, U>(
        string name, 
        T?[] data, 
        Func<T?[], (U[] values, byte[]? mask)> convFunc,
        Func<string, U[], byte[]?, SeriesHandle> wrapperFunc)
        where T : struct
    {
        var (vals, mask) = convFunc(data);
        return wrapperFunc(name, vals, mask);
    }
    /// <summary>
    /// Core Factory: Converts a raw C# Array into a SeriesHandle using low-level Packers.
    /// Used primarily by StructPacker for converting object properties to Series columns.
    /// </summary>
    public static SeriesHandle FromGenericArray(string name, Array array)
    {
        var handle = array switch
        {
            // ==========================================
            //  Signed Integers 
            // =========================================
            sbyte[] v   => PolarsWrapper.SeriesNew(name, v, null),
            sbyte?[] v  => NewNullable(name, v, arr => ArrayHelper.UnzipNullable(arr), PolarsWrapper.SeriesNew),
            
            short[] v   => PolarsWrapper.SeriesNew(name, v, null),
            short?[] v  => NewNullable(name, v, arr => ArrayHelper.UnzipNullable(arr), PolarsWrapper.SeriesNew),
            
            int[] v     => PolarsWrapper.SeriesNew(name, v, null),
            int?[] v    => NewNullable(name, v, arr => ArrayHelper.UnzipNullable(arr), PolarsWrapper.SeriesNew),
            
            long[] v    => PolarsWrapper.SeriesNew(name, v, null),
            long?[] v   => NewNullable(name, v, arr => ArrayHelper.UnzipNullable(arr), PolarsWrapper.SeriesNew),

            Int128[] v  => PolarsWrapper.SeriesNew(name, v, null),
            Int128?[] v => NewNullable(name, v, arr => ArrayHelper.UnzipNullable(arr), PolarsWrapper.SeriesNew),

            // ==========================================
            // 2. Unsigned Integers
            // ==========================================
            byte[] v    => PolarsWrapper.SeriesNew(name, v, null),
            byte?[] v   => NewNullable(name, v, arr => ArrayHelper.UnzipNullable(arr), PolarsWrapper.SeriesNew),

            ushort[] v  => PolarsWrapper.SeriesNew(name, v, null),
            ushort?[] v => NewNullable(name, v, arr => ArrayHelper.UnzipNullable(arr), PolarsWrapper.SeriesNew),

            uint[] v    => PolarsWrapper.SeriesNew(name, v, null),
            uint?[] v   => NewNullable(name, v, arr => ArrayHelper.UnzipNullable(arr), PolarsWrapper.SeriesNew),

            ulong[] v   => PolarsWrapper.SeriesNew(name, v, null),
            ulong?[] v  => NewNullable(name, v, arr => ArrayHelper.UnzipNullable(arr), PolarsWrapper.SeriesNew),

            UInt128[] v  => PolarsWrapper.SeriesNew(name, v, null),
            UInt128?[] v => NewNullable(name, v, arr => ArrayHelper.UnzipNullable(arr), PolarsWrapper.SeriesNew),

            // ==========================================
            // 3. Floating Point
            // ==========================================
            float[] v   => PolarsWrapper.SeriesNew(name, v, null),
            float?[] v  => NewNullable(name, v, 
                arr => ArrayHelper.UnzipNullable(arr, float.NaN), 
                PolarsWrapper.SeriesNew),

            double[] v  => PolarsWrapper.SeriesNew(name, v, null),
            double?[] v => NewNullable(name, v, 
                arr => ArrayHelper.UnzipNullable(arr, double.NaN), 
                PolarsWrapper.SeriesNew),

            // ==========================================
            // 4. Decimal & Bool 
            // ==========================================
            decimal[] v => PackDecimal(name, v),
            decimal?[] v => PackDecimalNullable(name, v),

            bool[] v => PackBool(name, v),
            bool?[] v => PackBoolNullable(name, v),
            
            string[] v => PolarsWrapper.SeriesNewStringSimd(name, v),

            // ==========================================
            // 7. Temporal
            // ==========================================
            
            // DateOnly -> i32 (Days)
            // T=DateOnly, U=int
            DateOnly[] v  => WithHelper(name, v, ArrayHelper.UnzipDateOnlyToInt32, PolarsWrapper.SeriesNewDate),
            DateOnly?[] v => WithHelperNullable(name, v, ArrayHelper.UnzipDateOnlyToInt32, PolarsWrapper.SeriesNewDate),

            // TimeOnly -> i64 (Ns)
            // T=TimeOnly, U=long
            TimeOnly[] v  => WithHelper(name, v, ArrayHelper.UnzipTimeOnlyToNs, PolarsWrapper.SeriesNewTime),
            TimeOnly?[] v => WithHelperNullable(name, v, ArrayHelper.UnzipTimeOnlyToNs, PolarsWrapper.SeriesNewTime),

            // TimeSpan -> i64 (Us)
            TimeSpan[] v  => WithHelper(name, v, ArrayHelper.UnzipTimeSpanToUs, PolarsWrapper.SeriesNewDuration),
            TimeSpan?[] v => WithHelperNullable(name, v, ArrayHelper.UnzipTimeSpanToUs, PolarsWrapper.SeriesNewDuration),

            // DateTime / Offset
            DateTime[] v => CreateDateTime(name, v),
            DateTime?[] v => CreateDateTimeNullable(name, v),
            DateTimeOffset[] v => CreateDateTimeOffset(name, v),
            DateTimeOffset?[] v => CreateDateTimeOffsetNullable(name, v),

            _ => null
        };

        if (handle != null && !handle.IsInvalid)
        {
            return handle;
        }

        return CreateFromArrowViaReflection(name, array);
    }

    private static SeriesHandle CreateFromArrowViaReflection(string name, Array array)
    {
        using var arrowArray = ArrowConverter.Build((dynamic)array); 
        return ArrowFfiBridge.ImportSeries(name, arrowArray);
    }

    private static SeriesHandle PackDecimal(string name, decimal[] v)
    {
        var (vals, scale) = DecimalPacker.Pack(v);
        return PolarsWrapper.SeriesNewDecimal(name, vals, null, scale);
    }
    
    private static SeriesHandle PackDecimalNullable(string name, decimal?[] v)
    {
        var (vals, mask, scale) = DecimalPacker.Pack(v);
        return PolarsWrapper.SeriesNewDecimal(name, vals, mask, scale);
    }
    
    private static SeriesHandle PackBool(string name, bool[] v)
    {
         var packed = BoolPacker.Pack(v);
         return PolarsWrapper.SeriesNew(name, packed, null, (nuint)v.Length);
    }
    
    private static SeriesHandle PackBoolNullable(string name, bool?[] v)
    {
         var (packed, mask) = BoolPacker.PackNullable(v);
         return PolarsWrapper.SeriesNew(name, packed, mask, (nuint)v.Length);
    }
    
    private static SeriesHandle CreateDateTime(string name, DateTime[] v)
    {
        var vals = ArrayHelper.UnzipDateTimeToUs(v);
        return PolarsWrapper.SeriesNewDatetime(name, vals, null, null);
    }
    
    private static SeriesHandle CreateDateTimeNullable(string name, DateTime?[] v)
    {
        var (vals, mask) = ArrayHelper.UnzipDateTimeToUs(v);
        return PolarsWrapper.SeriesNewDatetime(name, vals, mask, null);
    }
    
    private static SeriesHandle CreateDateTimeOffset(string name, DateTimeOffset[] v)
    {
        var vals = ArrayHelper.UnzipDateTimeOffsetToUs(v);
        return PolarsWrapper.SeriesNewDatetime(name, vals, null, "UTC"); 
    }
    
    private static SeriesHandle CreateDateTimeOffsetNullable(string name, DateTimeOffset?[] v)
    {
        var (vals, mask) = ArrayHelper.UnzipDateTimeOffsetToUs(v);
        return PolarsWrapper.SeriesNewDatetime(name, vals, mask, "UTC");
    }
}