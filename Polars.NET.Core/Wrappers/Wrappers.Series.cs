using Apache.Arrow;
using Apache.Arrow.C; 

namespace Polars.NET.Core;

public static partial class PolarsWrapper
{
    private static byte[]? ToBytes(bool[]? bools)
    {
        if (bools == null) return null;
        var bytes = new byte[bools.Length];
        for (int i = 0; i < bools.Length; i++)
        {
            bytes[i] = bools[i] ? (byte)1 : (byte)0;
        }
        return bytes;
    }
    // --- Constructors ---

    public static SeriesHandle SeriesNew(string name, int[] data, bool[]? validity) => 
        ErrorHelper.Check(NativeBindings.pl_series_new_i32(name, data, ToBytes(validity), (UIntPtr)data.Length));
    public static SeriesHandle SeriesNew(string name, long[] data, bool[]? validity) => 
        ErrorHelper.Check(NativeBindings.pl_series_new_i64(name, data, ToBytes(validity), (UIntPtr)data.Length));

    public static SeriesHandle SeriesNew(string name, double[] data, bool[]? validity) => 
        ErrorHelper.Check(NativeBindings.pl_series_new_f64(name, data, ToBytes(validity), (UIntPtr)data.Length));
        
    public static SeriesHandle SeriesNew(string name, bool[] data, bool[]? validity)
    {
        var dataBytes = ToBytes(data)!; 
        var validBytes = ToBytes(validity);
        return ErrorHelper.Check(NativeBindings.pl_series_new_bool(name, dataBytes, validBytes, (UIntPtr)data.Length));
    }

    public static SeriesHandle SeriesNew(string name, string?[] data)
    {
        return UseNullableUtf8StringArray(data, ptrs => 
        {
            return ErrorHelper.Check(
                NativeBindings.pl_series_new_str(
                    name, 
                    ptrs, 
                    (UIntPtr)data.Length
                )
            );
        });
    }
    
    private static readonly decimal[] PowersOf10;

    static PolarsWrapper()
    {
        PowersOf10 = new decimal[29]; // 0 .. 28
        PowersOf10[0] = 1m;
        for (int i = 1; i < PowersOf10.Length; i++)
        {
            PowersOf10[i] = PowersOf10[i - 1] * 10m;
        }
    }

    public static SeriesHandle SeriesNewDecimal(string name, decimal[] data, bool[]? validity, int scale)
    {
        if (scale < 0 || scale >= PowersOf10.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(scale), $"Scale must be between 0 and {PowersOf10.Length - 1} for C# decimal conversion.");
        }
        var len = data.Length;
        var scaledValues = new Int128[len];
        var multiplier = PowersOf10[scale];

        // Convert C# decimal to Int128
        for (int i = 0; i < len; i++)
        {
            scaledValues[i] = (Int128)(data[i] * multiplier);
        }

        return ErrorHelper.Check(NativeBindings.pl_series_new_decimal(
            name, 
            scaledValues, 
            ToBytes(validity), 
            (UIntPtr)len, 
            (UIntPtr)scale
        ));
    }
    
    // Nullable Decimal
    public static SeriesHandle SeriesNewDecimal(string name, decimal?[] data, int scale)
    {
        if (scale < 0 || scale >= PowersOf10.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(scale), $"Scale must be between 0 and {PowersOf10.Length - 1} for C# decimal conversion.");
        }
        var len = data.Length;
        var scaledValues = new Int128[len];
        var validity = new byte[len];
        var multiplier = PowersOf10[scale];

        for (int i = 0; i < len; i++)
        {
            var value = data[i];
            if (value.HasValue)
            {
                scaledValues[i] = (Int128)(value.Value * multiplier);
                validity[i] = 1;
            }
            else
            {
                scaledValues[i] = 0;
                validity[i] = 0;
            }
        }

        return ErrorHelper.Check(NativeBindings.pl_series_new_decimal(
            name, 
            scaledValues, 
            validity, 
            (UIntPtr)len, 
            (UIntPtr)scale
        ));
    }
    public static SeriesHandle CloneSeries(SeriesHandle handle)
    {
        return ErrorHelper.Check(NativeBindings.pl_series_clone(handle));
    }
    // --- Properties ---
    public static string GetSeriesDtypeString(SeriesHandle h)
    {
        var ptr = NativeBindings.pl_series_dtype_str(h);
        return ErrorHelper.CheckString(ptr);
    }
    /// <summary>
    /// Get DataType Handle from Series
    /// </summary>
    public static DataTypeHandle GetSeriesDataType(SeriesHandle handle)
    {
        return ErrorHelper.Check(NativeBindings.pl_series_get_dtype(handle));
    }
    public static long SeriesLen(SeriesHandle h) => (long)NativeBindings.pl_series_len(h);
    
    public static string SeriesName(SeriesHandle h) 
    {
        var ptr = NativeBindings.pl_series_name(h);
        return ErrorHelper.CheckString(ptr) ;
    }
    
    public static void SeriesRename(SeriesHandle h, string name) => NativeBindings.pl_series_rename(h, name);

    // --- DataFrame Conversion ---
    public static DataFrameHandle SeriesToFrame(SeriesHandle h) 
    {
        return ErrorHelper.Check(NativeBindings.pl_series_to_frame(h));
    }
    public static long? SeriesGetInt(SeriesHandle s, long idx)
    {
        if (NativeBindings.pl_series_get_i64(s, (UIntPtr)idx, out long val)) return val;
        return null;
    }

    public static double? SeriesGetDouble(SeriesHandle s, long idx)
    {
        if (NativeBindings.pl_series_get_f64(s, (UIntPtr)idx, out double val)) return val;
        return null;
    }

    public static bool? SeriesGetBool(SeriesHandle s, long idx)
    {
        if (NativeBindings.pl_series_get_bool(s, (UIntPtr)idx, out bool val)) return val;
        return null;
    }

    public static string? SeriesGetString(SeriesHandle s, long idx)
    {
        IntPtr ptr = NativeBindings.pl_series_get_str(s, (UIntPtr)idx);
        return ErrorHelper.CheckString(ptr); // CheckString 会处理 IntPtr.Zero -> null
    }

    public static decimal? SeriesGetDecimal(SeriesHandle s, long idx)
    {
        if (NativeBindings.pl_series_get_decimal(s, (UIntPtr)idx, out Int128 val, out UIntPtr scalePtr))
        {
            int scale = (int)scalePtr;
            
            try 
            {
                decimal d = (decimal)val; 
                
                if (scale >= 0 && scale < PowersOf10.Length)
                {
                    return d / PowersOf10[scale];
                }
                else
                {
                    return d / (decimal)Math.Pow(10, scale);
                }
            }
            catch (OverflowException)
            {
                return null;
            }
        }
        return null;
    }
    // Date: Days since 1970-01-01
    public static DateOnly? SeriesGetDate(SeriesHandle s, long idx)
    {
        if (NativeBindings.pl_series_get_date(s, (UIntPtr)idx, out int days))
        {
            // 719162 is days from 0001-01-01 to 1970-01-01
            return DateOnly.FromDayNumber(days + 719162); 
        }
        return null;
    }

    // Time: Nanoseconds since midnight
    public static TimeOnly? SeriesGetTime(SeriesHandle s, long idx)
    {
        if (NativeBindings.pl_series_get_time(s, (UIntPtr)idx, out long ns))
        {
            // .NET Ticks = 100ns
            long ticks = ns / 100;
            return new TimeOnly(ticks);
        }
        return null;
    }

    // Datetime: Microseconds since 1970-01-01 (Assuming 'us' time unit)
    public static DateTime? SeriesGetDatetime(SeriesHandle s, long idx)
    {
        if (NativeBindings.pl_series_get_datetime(s, (UIntPtr)idx, out long us))
        {
            // .NET Ticks = 100ns. 1 us = 10 ticks.
            // Unix Epoch Ticks = 621355968000000000
            long ticks = (us * 10) + 621355968000000000L;
            return new DateTime(ticks, DateTimeKind.Utc); // 默认 UTC 语义
        }
        return null;
    }

    // Duration: Microseconds (Assuming 'us')
    public static TimeSpan? SeriesGetDuration(SeriesHandle s, long idx)
    {
        if (NativeBindings.pl_series_get_duration(s, (UIntPtr)idx, out long us))
        {
            // 1 us = 10 ticks
            return new TimeSpan(us * 10);
        }
        return null;
    }
    // --- Arrow Integration ---

    public static unsafe IArrowArray SeriesToArrow(SeriesHandle h)
    {
        using var contextHandle = NativeBindings.pl_series_to_arrow(h);
        
        var cArray = new CArrowArray();
        var cSchema = new CArrowSchema();
        
        NativeBindings.pl_arrow_array_export(contextHandle, &cArray);
        NativeBindings.pl_arrow_schema_export(contextHandle, &cSchema);
        bool ownershipTransferred = false;
        try
        {
            var importedField = CArrowSchemaImporter.ImportField(&cSchema);
            
            var array = CArrowArrayImporter.ImportArray(&cArray, importedField.DataType);
            ownershipTransferred = true;
            return array;
        }
        finally
        {
            if (!ownershipTransferred)
            {
                CArrowArray.Free(&cArray);
                CArrowSchema.Free(&cSchema);
            }
        }
    }
    /// <summary>
    /// Imports an Arrow Array via C Data Interface.
    /// </summary>
    public static unsafe SeriesHandle SeriesFromArrow(string name, CArrowArray* cArray, CArrowSchema* cSchema)
        => ErrorHelper.Check(NativeBindings.pl_arrow_to_series(name, cArray, cSchema));
    public static SeriesHandle SeriesCast(SeriesHandle s, DataTypeHandle dtype)
        => ErrorHelper.Check(NativeBindings.pl_series_cast(s, dtype));
    public static SeriesHandle SeriesIsNull(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_is_null(s));
    public static SeriesHandle SeriesIsNotNull(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_is_not_null(s));
    public static bool SeriesIsNullAt(SeriesHandle s, long idx) => NativeBindings.pl_series_is_null_at(s, (UIntPtr)idx);
    public static SeriesHandle SeriesIsNan(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_is_nan(s));
    public static SeriesHandle SeriesIsNotNan(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_is_not_nan(s));
    public static SeriesHandle SeriesIsFinite(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_is_finite(s));
    public static SeriesHandle SeriesIsInfinite(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_is_infinite(s));
    public static long SeriesNullCount(SeriesHandle s) => (long)NativeBindings.pl_series_null_count(s);
    public static SeriesHandle SeriesUnique(SeriesHandle handle) => ErrorHelper.Check(NativeBindings.pl_series_unique(handle));
    public static SeriesHandle SeriesUniqueStable(SeriesHandle handle) => ErrorHelper.Check(NativeBindings.pl_series_unique_stable(handle));
    public static ulong SeriesNUnique(SeriesHandle handle) => NativeBindings.pl_series_n_unique(handle);
    // Ops
    public static SeriesHandle SeriesAdd(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_add(s1, s2));
    public static SeriesHandle SeriesSub(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_sub(s1, s2));
    public static SeriesHandle SeriesMul(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_mul(s1, s2));
    public static SeriesHandle SeriesDiv(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_div(s1, s2));

    public static SeriesHandle SeriesEq(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_eq(s1, s2));
    public static SeriesHandle SeriesNeq(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_neq(s1, s2));
    public static SeriesHandle SeriesGt(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_gt(s1, s2));
    public static SeriesHandle SeriesLt(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_lt(s1, s2));
    public static SeriesHandle SeriesGtEq(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_gt_eq(s1, s2));
    public static SeriesHandle SeriesLtEq(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_lt_eq(s1, s2));

    // Aggs
    public static SeriesHandle SeriesSum(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_sum(s));
    public static SeriesHandle SeriesMean(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_mean(s));
    public static SeriesHandle SeriesMin(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_min(s));
    public static SeriesHandle SeriesMax(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_max(s));
    // Slice
    public static SeriesHandle SeriesSlice(SeriesHandle handle, long offset, long length)
        => ErrorHelper.Check(NativeBindings.pl_series_slice(handle, offset, (UIntPtr)length));
    // Sort
    public static SeriesHandle SeriesSort(
        SeriesHandle series, 
        bool descending = false,
        bool nullsLast = false,
        bool multithreaded = true, 
        bool maintainOrder = false
    )
    {
        return ErrorHelper.Check(NativeBindings.pl_series_sort(
            series, 
            descending, 
            nullsLast, 
            multithreaded, 
            maintainOrder
        ));
    }
    public static DataFrameHandle SeriesStructUnnest(SeriesHandle series)   
        => ErrorHelper.Check(NativeBindings.pl_series_struct_unnest(series));
}