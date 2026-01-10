namespace Polars.NET.Core;

public static partial class PolarsWrapper
{
    // ==========================================
    // Metadata
    // ==========================================
    public static long DataFrameHeight(DataFrameHandle df) => (long)NativeBindings.pl_dataframe_height(df);
    public static long DataFrameWidth(DataFrameHandle df) => (long)NativeBindings.pl_dataframe_width(df);
    public static string[] GetColumnNames(DataFrameHandle df)
    {
        long width = DataFrameWidth(df);
        var names = new string[width];
        
        for (long i = 0; i < width; i++)
        {
            IntPtr ptr = NativeBindings.pl_dataframe_get_column_name(df, (UIntPtr)i);
            
            names[i] = ErrorHelper.CheckString(ptr);
        }
        return names;
    }
    public static string GetDataFrameSchemaString(DataFrameHandle h)
    {
        var ptr = NativeBindings.pl_dataframe_schema(h);
        return ErrorHelper.CheckString(ptr);
    }
    public static DataFrameHandle CloneDataFrame(DataFrameHandle df)
    {
        return ErrorHelper.Check(NativeBindings.pl_dataframe_clone(df));
    }
    // ==========================================
    // Scalar Access
    // ==========================================

    public static long? GetInt(DataFrameHandle df, string colName, long row)
    {
        if (NativeBindings.pl_dataframe_get_i64(df, colName, (UIntPtr)row, out long val))
        {
            return val;
        }
        return null;
    }

    public static double? GetDouble(DataFrameHandle df, string colName, long row)
    {
        if (NativeBindings.pl_dataframe_get_f64(df, colName, (UIntPtr)row, out double val))
        {
            return val;
        }
        return null;
    }

    public static string? GetString(DataFrameHandle df, string colName, long row)
    {
        IntPtr ptr = NativeBindings.pl_dataframe_get_string(df, colName, (UIntPtr)row);
        return ErrorHelper.CheckString(ptr);
    }
    // ==========================================
    // Eager Ops
    // ==========================================

    public static DataFrameHandle Head(DataFrameHandle df, uint n)
        => ErrorHelper.Check(NativeBindings.pl_head(df, n));

    public static DataFrameHandle Tail(DataFrameHandle df, uint n)
        => ErrorHelper.Check(NativeBindings.pl_tail(df, n));
    public static DataFrameHandle Drop(DataFrameHandle df, string name)
        => ErrorHelper.Check(NativeBindings.pl_dataframe_drop(df, name));

    public static DataFrameHandle Rename(DataFrameHandle df, string oldName, string newName)
        => ErrorHelper.Check(NativeBindings.pl_dataframe_rename(df, oldName, newName));

    public static DataFrameHandle DropNulls(DataFrameHandle df, string[]? subset)
    {
        return UseUtf8StringArray(subset ?? [], ptrs => 
        {
            return ErrorHelper.Check(NativeBindings.pl_dataframe_drop_nulls(df, ptrs, (UIntPtr)ptrs.Length));
        });
    }

    public static unsafe DataFrameHandle SampleN(DataFrameHandle df, ulong n, bool replacement, bool shuffle, ulong? seed)
    {
        ulong sVal = seed ?? 0;
        ulong* sPtr = seed.HasValue ? &sVal : null;
        return ErrorHelper.Check(NativeBindings.pl_dataframe_sample_n(df, (UIntPtr)n, replacement, shuffle, sPtr));
    }

    public static unsafe DataFrameHandle SampleFrac(DataFrameHandle df, double frac, bool replacement, bool shuffle, ulong? seed)
    {
        ulong sVal = seed ?? 0;
        ulong* sPtr = seed.HasValue ? &sVal : null;
        return ErrorHelper.Check(NativeBindings.pl_dataframe_sample_frac(df, frac, replacement, shuffle, sPtr));
    }
    public static DataFrameHandle Filter(DataFrameHandle df, ExprHandle expr)
    {
        var h = NativeBindings.pl_filter(df, expr);
        expr.TransferOwnership(); 
        return ErrorHelper.Check(h);
    }
    public static DataFrameHandle WithColumns(DataFrameHandle df, ExprHandle[] exprs)
    {
        var raw = HandlesToPtrs(exprs);
        return ErrorHelper.Check(NativeBindings.pl_with_columns(df, raw, (UIntPtr)raw.Length));
    }
    public static DataFrameHandle Select(DataFrameHandle df, ExprHandle[] exprs)
    {
        var rawExprs = HandlesToPtrs(exprs);
        return ErrorHelper.Check(NativeBindings.pl_select(df, rawExprs, (UIntPtr)rawExprs.Length));
    }

    public static DataFrameHandle Join(DataFrameHandle left, DataFrameHandle right, ExprHandle[] leftOn, ExprHandle[] rightOn, PlJoinType how)
    {
        var lPtrs = HandlesToPtrs(leftOn);
        var rPtrs = HandlesToPtrs(rightOn);
        return ErrorHelper.Check(NativeBindings.pl_join(left, right, lPtrs, (UIntPtr)lPtrs.Length, rPtrs, (UIntPtr)rPtrs.Length, how));
    }
    public static DataFrameHandle DataFrameSort(
        DataFrameHandle df, 
        ExprHandle[] exprs, 
        bool[] descending,
        bool[] nullsLast,
        bool maintainOrder)
    {
        if ((descending.Length != 1 && descending.Length != exprs.Length) ||
            (nullsLast.Length != 1 && nullsLast.Length != exprs.Length))
        {
                throw new ArgumentException("Sort options length mismatch.");
        }

        var exprPtrs = HandlesToPtrs(exprs);

        unsafe
        {
            fixed (bool* descPtr = descending)
            fixed (bool* nullsPtr = nullsLast)
            {
                return ErrorHelper.Check(NativeBindings.pl_dataframe_sort(
                    df,
                    exprPtrs,
                    (UIntPtr)exprs.Length,
                    descPtr,
                    (UIntPtr)descending.Length,
                    nullsPtr,
                    (UIntPtr)nullsLast.Length,
                    maintainOrder
                ));
            }
        }
    }
    public static DataFrameHandle Explode(DataFrameHandle df, ExprHandle[] exprs)
    {
        var raw = HandlesToPtrs(exprs);
        return ErrorHelper.Check(NativeBindings.pl_explode(df, raw, (UIntPtr)raw.Length));
    }
    public static DataFrameHandle Unnest(DataFrameHandle df, string[] columns)
        => ErrorHelper.Check(NativeBindings.pl_dataframe_unnest(df, columns, (UIntPtr)columns.Length));
    // GroupBy
    public static DataFrameHandle GroupByAgg(DataFrameHandle df, ExprHandle[] by, ExprHandle[] agg)
    {
        var rawBy = HandlesToPtrs(by);
        var rawAgg = HandlesToPtrs(agg);
        return ErrorHelper.Check(NativeBindings.pl_groupby_agg(
            df, 
            rawBy, (UIntPtr)rawBy.Length,
            rawAgg, (UIntPtr)rawAgg.Length
        ));
    }
    // Pivot (Eager)
    public static DataFrameHandle Pivot(DataFrameHandle df, string[] index, string[] columns, string[] values, PlPivotAgg aggFn)
    {
        return UseUtf8StringArray(index, iPtrs =>
            UseUtf8StringArray(columns, cPtrs =>
                UseUtf8StringArray(values, vPtrs =>
                {
                    return ErrorHelper.Check(NativeBindings.pl_pivot(
                        df,
                        vPtrs, (UIntPtr)vPtrs.Length,
                        iPtrs, (UIntPtr)iPtrs.Length,
                        cPtrs, (UIntPtr)cPtrs.Length,
                        aggFn
                    ));
                })
            )
        );
    }

    // Unpivot (Eager)
    public static DataFrameHandle Unpivot(DataFrameHandle df, string[] index, string[] on, string? variableName, string? valueName)
    {
        return UseUtf8StringArray(index, iPtrs =>
            UseUtf8StringArray(on, oPtrs =>
            {
                return ErrorHelper.Check(NativeBindings.pl_unpivot(
                    df,
                    iPtrs, (UIntPtr)iPtrs.Length,
                    oPtrs, (UIntPtr)oPtrs.Length,
                    variableName,
                    valueName
                ));
            })
        );
    }
    public static DataFrameHandle Concat(DataFrameHandle[] handles, PlConcatType how)
    {
        var ptrs = HandlesToPtrs(handles);
        
        var h = NativeBindings.pl_concat(ptrs, (UIntPtr)ptrs.Length, how);

        foreach (var handle in handles)
        {
            handle.TransferOwnership();
        }

        return ErrorHelper.Check(h);
    }
    public static SeriesHandle DataFrameGetColumn(DataFrameHandle h, string name)
    {
        var sh = NativeBindings.pl_dataframe_get_column(h, name);
        if (sh.IsInvalid)
        {
            throw new ArgumentException($"Column '{name}' not found in DataFrame.");
        }
        return sh;
    }

    // Get by Index
    public static SeriesHandle DataFrameGetColumnAt(DataFrameHandle h, int index)
    {
        var sh = NativeBindings.pl_dataframe_get_column_at(h, (UIntPtr)index);
        if (sh.IsInvalid)
        {
            throw new IndexOutOfRangeException($"Column index {index} is out of bounds.");
        }
        return sh;
    }
    public static DataFrameHandle DataFrameNew(SeriesHandle[] series)
    {
        if (series == null || series.Length == 0)
            {
                return ErrorHelper.Check(NativeBindings.pl_dataframe_new(Array.Empty<IntPtr>(), UIntPtr.Zero));
            }

        using var locker = new SafeHandleLock<SeriesHandle>(series);

        return ErrorHelper.Check(NativeBindings.pl_dataframe_new(locker.Pointers, (UIntPtr)series.Length));
    }
    /// <summary>
    /// Create a DataFrame from an Arrow C Stream.
    /// </summary>
    public static unsafe DataFrameHandle DataFrameNewFromStream(Arrow.CArrowArrayStream* stream)
    {
        var handle = NativeBindings.pl_dataframe_new_from_stream(stream);
        return ErrorHelper.Check(handle);
    }
    /// <summary>
    /// Convert DataFrame to LazyFrame
    /// </summary>
    /// <param name="df"></param>
    /// <returns></returns>
    public static LazyFrameHandle DataFrameToLazy(DataFrameHandle df) 
        => ErrorHelper.Check(NativeBindings.pl_dataframe_lazy(df));
    public static string DataFrameToString(DataFrameHandle handle)
    {
        var ptr = NativeBindings.pl_dataframe_to_string(handle);
        return ErrorHelper.CheckString(ptr);
    }
}
