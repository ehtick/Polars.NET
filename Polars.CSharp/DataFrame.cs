using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using Apache.Arrow;
using System.Data;
using Polars.NET.Core.Data;
using System.Collections.Concurrent;
using System.Collections;
using System.Text;

namespace Polars.CSharp;

/// <summary>
/// DataFrame represents a 2-dimensional labeled data structure similar to a table or spreadsheet.
/// </summary>
public class DataFrame : IDisposable,IEnumerable<Series>
{
    internal DataFrameHandle Handle { get; }

    internal DataFrame(DataFrameHandle handle)
    {
        Handle = handle;
    }

    // ==========================================
    // Metadata
    // ==========================================

    /// <summary>
    /// Get the schema of the DataFrame as a dictionary (Column Name -> Data Type String).
    /// </summary>
    public Dictionary<string, DataType> Schema
    {
        get
        {
            int width = (int)PolarsWrapper.DataFrameWidth(Handle);
            var schema = new Dictionary<string, DataType>(width);

            for (int i = 0; i < width; i++)
            {
                // Get Series
                using var seriesHandle = PolarsWrapper.DataFrameGetColumnAt(Handle, i);
                
                // Get Column Name
                string name = PolarsWrapper.SeriesName(seriesHandle);

                // Get DataType
                var dtHandle = PolarsWrapper.GetSeriesDataType(seriesHandle);
                
                // 4. Build DataType object
                schema[name] = new DataType(dtHandle);
            }

            return schema;
        }
    }
    /// <summary>
    /// Prints the schema to the console in a tree format.
    /// Useful for debugging column names and data types.
    /// </summary>
    public void PrintSchema()
    {
        var schema = Schema; 
        
        Console.WriteLine("root");
        foreach (var kvp in schema)
        {
            Console.WriteLine($" |-- {kvp.Key}: {kvp.Value}");
        }
    }
    // ==========================================
    // Static IO Read
    // ==========================================
    /// <summary>
    /// Reads a CSV file into a DataFrame.
    /// </summary>
    /// <param name="path">Path to the CSV file.</param>
    /// <param name="schema">Optional schema dictionary.</param>
    /// <param name="hasHeader">Whether the CSV has a header row.</param>
    /// <param name="separator">Character used as separator.</param>
    /// <param name="skipRows">Choose how many rows should be skipped.</param>
    /// <param name="tryParseDates">Whether to automatically try parsing dates/datetimes. Default is true.</param>
    /// <returns>A new DataFrame.</returns>
    public static DataFrame ReadCsv(
        string path, 
        Dictionary<string, DataType>? schema = null,
        bool hasHeader = true, 
        char separator = ',',
        ulong skipRows = 0,
        bool tryParseDates = true) 
    {
        var schemaHandles = schema?.ToDictionary(
            kv => kv.Key, 
            kv => kv.Value.Handle
        );

        var handle = PolarsWrapper.ReadCsv(
            path, 
            schemaHandles, 
            hasHeader, 
            separator, 
            skipRows,
            tryParseDates
        );

        return new DataFrame(handle);
    }
    /// <summary>
    /// Read Parquet File
    /// </summary>
    /// <param name="path"></param>
    /// <returns></returns>
    public static DataFrame ReadParquet(string path)
    {
        //
        return new DataFrame(PolarsWrapper.ReadParquet(path));
    }
    /// <summary>
    /// Read JSON File
    /// </summary>
    /// <param name="path"></param>
    /// <returns></returns>
    public static DataFrame ReadJson(string path)
    {
        //
        return new DataFrame(PolarsWrapper.ReadJson(path));
    }
    /// <summary>
    /// Read IPC File
    /// </summary>
    /// <param name="path"></param>
    /// <returns></returns>
    public static DataFrame ReadIpc(string path)
    {
        return new DataFrame(PolarsWrapper.ReadIpc(path));
    }

    /// <summary>
    /// Create DataFrame from Apache Arrow RecordBatch.
    /// </summary>
    public static DataFrame FromArrow(RecordBatch batch)
    {
        var handle = ArrowFfiBridge.ImportDataFrame(batch);
        return new DataFrame(handle);
    }
    /// <summary>
    /// Transfer a RecordBatch to Arrow
    /// </summary>
    /// <returns></returns>
    public RecordBatch ToArrow()
    {
        return ArrowFfiBridge.ExportDataFrame(Handle);
    }
    /// <summary>
    /// Asynchronously reads a CSV file into a DataFrame.
    /// </summary>
    public static async Task<DataFrame> ReadCsvAsync(
        string path,
        Dictionary<string, DataType>? schema = null,
        bool hasHeader = true,
        char separator = ',',
        ulong skipRows = 0,
        bool tryParseDates = true) 
    {
        var schemaHandles = schema?.ToDictionary(
            kv => kv.Key, 
            kv => kv.Value.Handle
        );

        var handle = await PolarsWrapper.ReadCsvAsync(
            path, 
            schemaHandles, 
            hasHeader, 
            separator, 
            skipRows,
            tryParseDates 
        );

        return new DataFrame(handle);
    }
    /// <summary>
    /// Read a Parquet file asynchronously.
    /// </summary>
    public static async Task<DataFrame> ReadParquetAsync(string path)
    {
        var handle = await PolarsWrapper.ReadParquetAsync(path);
        return new DataFrame(handle);
    }

    /// <summary>
    /// Create a DataFrame directly from a DbDataReader.
    /// Supports basic types, Arrays (as List), and POCOs (as Struct).
    /// </summary>
    /// <param name="reader">The open data reader.</param>
    /// <param name="batchSize">Number of rows per Arrow batch.</param>
    public static DataFrame ReadDatabase(IDataReader reader, int batchSize = 50_000)
    {
        // Get Schema 
         var schema = reader.GetArrowSchema();

        // Get ArrowStream
        var batchEnumerable = reader.ToArrowBatches(batchSize);
        
        // Get Enumberator
        var enumerator = batchEnumerable.GetEnumerator();

        try 
        {
            var handle = ArrowStreamInterop.ImportEager(enumerator, schema);
            return new DataFrame(handle);
        }
        catch
        {
            enumerator.Dispose();
            throw;
        }
    }
    // ==========================================
    // Properties
    // ==========================================
    /// <summary>
    /// Return DataFrame Height
    /// </summary>
    public long Height => PolarsWrapper.DataFrameHeight(Handle);
    /// <summary>
    /// Return DataFrame Height
    /// </summary>
    public long Len => PolarsWrapper.DataFrameHeight(Handle); 
    /// <summary>
    /// Return DataFrame Width
    /// </summary>
    public long Width => PolarsWrapper.DataFrameWidth(Handle);  
    /// <summary>
    /// Return DataFrame Columns' Name
    /// </summary>
    public string[] Columns => PolarsWrapper.GetColumnNames(Handle);
    /// <summary>
    /// Get column names in order.
    /// </summary>
    public string[] ColumnNames => PolarsWrapper.GetColumnNames(Handle);

    // ==========================================
    // Scalar Access (Direct)
    // ==========================================

    /// <summary>
    /// Get a value from the DataFrame at the specified row and column.
    /// This is efficient for single-value lookups (no Arrow conversion).
    /// </summary>
    public T? GetValue<T>(long rowIndex, string colName)
    {
        var series = this[colName];
        
        return series.GetValue<T>(rowIndex);
    }
    /// <summary>
    /// Get a value from the DataFrame at the specified row and column.
    /// This is efficient for single-value lookups (no Arrow conversion).
    /// </summary>
    public T? GetValue<T>(string colName,long rowIndex)
    {
        var series = this[colName];
        return series.GetValue<T>(rowIndex);
    }

    /// <summary>
    /// Get value by row index and column name (object type).
    /// </summary>
    /// <param name="rowIndex"></param>
    /// <param name="colName"></param>
    /// <returns></returns>
    public object? this[int rowIndex, string colName]
    {
        get
        {
            var series = this[colName];
            return series[rowIndex];
        }
    }
    
    /// <summary>
    /// Get value by row index and column name (object type).
    /// </summary>
    /// <param name="rowIndex"></param>
    /// <param name="colName"></param>
    /// <returns></returns>
    public object? this[string colName,int rowIndex]
    {
        get
        {
            var series = this[colName];
            return series[rowIndex]; 
        }
    }
    // ==========================================
    // DataFrame Operations
    // ==========================================
    /// <summary>
    /// Select columns
    /// </summary>
    /// <param name="exprs"></param>
    /// <returns></returns>
    public DataFrame Select(params Expr[] exprs)
    {
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        return new DataFrame(PolarsWrapper.Select(Handle, handles));
    }
    /// <summary>
    /// Select columns by name.
    /// </summary>
    public DataFrame Select(params string[] columns)
    {
        var exprs = columns.Select(Polars.Col).ToArray();
        return Select(exprs);
    }
    /// <summary>
    /// Filter rows based on a boolean expression. 
    /// </summary>
    /// <param name="expr"></param>
    /// <returns></returns>
    public DataFrame Filter(Expr expr)
    {
        var h = PolarsWrapper.CloneExpr(expr.Handle);

        return new DataFrame(PolarsWrapper.Filter(Handle, h));
    }
    /// <summary>
    /// Filter rows based on a boolean expression. 
    /// </summary>
    /// <param name="exprs"></param>
    /// <returns></returns>
    public DataFrame WithColumns(params Expr[] exprs)
    {
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();

        return new DataFrame(PolarsWrapper.WithColumns(Handle, handles));
    }
    /// <summary>
    /// Sort the DataFrame by a single column.
    /// </summary>
    public DataFrame Sort(
        string column, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
    {
        return Sort(
            [Polars.Col(column)], 
            [descending], 
            [nullsLast], 
            maintainOrder
        );
    }
    /// <summary>
    /// Sort the DataFrame by a single expr.
    /// </summary>
    public DataFrame Sort(
        Expr expr, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
    {
        return Sort(
            [expr], 
            [descending], 
            [nullsLast], 
            maintainOrder
        );
    }
    /// <summary>
    /// Sort the DataFrame by multiple columns (all ascending or all descending).
    /// </summary>
    public DataFrame Sort(
        string[] columns, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
    {
        var exprs = columns.Select(Polars.Col).ToArray();
        return Sort(
            exprs, 
            [descending], 
            [nullsLast], 
            maintainOrder
        );
    }

    /// <summary>
    /// Sort the DataFrame by multiple columns with specific sort orders.
    /// </summary>
    public DataFrame Sort(
        string[] columns, 
        bool[] descending, 
        bool[] nullsLast, 
        bool maintainOrder = false)
    {
        var exprs = columns.Select(Polars.Col).ToArray();
        return Sort(exprs, descending, nullsLast, maintainOrder);
    }

    /// <summary>
    /// Sort using multiple expressions (all ascending or all descending).
    /// </summary>
    public DataFrame Sort(
        Expr[] exprs, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
    {
        return Sort(
            exprs, 
            [descending], 
            [nullsLast], 
            maintainOrder
        );
    }

    /// <summary>
    /// Sort the DataFrame by multiple columns.
    /// </summary>
    public DataFrame Sort(
        Expr[] exprs, 
        bool[] descending, 
        bool[] nullsLast, 
        bool maintainOrder = false)
    {
        var clonedHandles = new ExprHandle[exprs.Length];
        for (int i = 0; i < exprs.Length; i++)
        {
            clonedHandles[i] = PolarsWrapper.CloneExpr(exprs[i].Handle);
        }

        var h = PolarsWrapper.DataFrameSort(
            Handle, 
            clonedHandles, 
            descending, 
            nullsLast, 
            maintainOrder
        );
        
        return new DataFrame(h);
    }
    // ==========================================
    // TopK / BottomK (Eager Shortcuts)
    // ==========================================

    /// <summary>
    /// Get the top k rows according to the given expressions.
    /// <para>This selects the largest values.</para>
    /// </summary>
    public DataFrame TopK(int k, Expr[] by, bool[] reverse) => Lazy().TopK(k, by, reverse) .Collect();

    /// <summary>
    /// Get the top k rows according to a single expression.
    /// </summary>
    public DataFrame TopK(int k, Expr by, bool reverse = false) => Lazy().TopK(k, by, reverse).Collect();

    /// <summary>
    /// Get the top k rows according to a column name.
    /// </summary>
    public DataFrame TopK(int k, string colName, bool reverse = false) => Lazy().TopK(k, colName, reverse).Collect();

    /// <summary>
    /// Get the bottom k rows according to the given expressions.
    /// <para>This selects the smallest values.</para>
    /// </summary>
    public DataFrame BottomK(int k, Expr[] by, bool[] reverse) => Lazy().BottomK(k, by, reverse).Collect();

    /// <summary>
    /// Get the bottom k rows according to a single expression.
    /// </summary>
    public DataFrame BottomK(int k, Expr by, bool reverse = false) => Lazy().BottomK(k, by, reverse).Collect();

    /// <summary>
    /// Get the bottom k rows according to a column name.
    /// </summary>
    public DataFrame BottomK(int k, string colName, bool reverse = false) => Lazy().BottomK(k, colName, reverse).Collect();
    /// <summary>
    /// Return head lines from a DataFrame
    /// </summary>
    /// <param name="n"></param>
    /// <returns></returns>
    public DataFrame Head(int n = 5) => new(PolarsWrapper.Head(Handle, (uint)n));
    /// <summary>
    /// Return tail lines from a DataFrame
    /// </summary>
    /// <param name="n"></param>
    /// <returns></returns>
    public DataFrame Tail(int n = 5) => new(PolarsWrapper.Tail(Handle, (uint)n));
    /// <summary>
    /// Explode a list or structure in a Column
    /// </summary>
    /// <param name="exprs"></param>
    /// <returns></returns>
    public DataFrame Explode(params Expr[] exprs)
    {
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        return new DataFrame(PolarsWrapper.Explode(Handle, handles));
    }
    /// <summary>
    /// Decompose a struct column into multiple columns.
    /// </summary>
    /// <param name="columns">The struct columns to unnest.</param>
    public DataFrame Unnest(params string[] columns)
    {
        var newHandle = PolarsWrapper.Unnest(Handle, columns);
        return new DataFrame(newHandle);
    }
    // ==========================================
    // Data Cleaning / Structure Ops
    // ==========================================

    /// <summary>
    /// Drop a column by name.
    /// </summary>
    public DataFrame Drop(string columnName) => new(PolarsWrapper.Drop(Handle, columnName));

    /// <summary>
    /// Rename a column.
    /// </summary>
    public DataFrame Rename(string oldName, string newName) => new(PolarsWrapper.Rename(Handle, oldName, newName));

    /// <summary>
    /// Drop rows containing null values.
    /// </summary>
    /// <param name="subset">Column names to consider. If null/empty, checks all columns.</param>
    public DataFrame DropNulls(params string[]? subset) => new(PolarsWrapper.DropNulls(Handle, subset));

    // ==========================================
    // Sampling
    // ==========================================

    /// <summary>
    /// Sample n rows from the DataFrame.
    /// </summary>
    public DataFrame Sample(ulong n, bool withReplacement = false, bool shuffle = true, ulong? seed = null)
        => new(PolarsWrapper.SampleN(Handle, n, withReplacement, shuffle, seed));

    /// <summary>
    /// Sample a fraction of rows from the DataFrame.
    /// </summary>
    public DataFrame Sample(double fraction, bool withReplacement = false, bool shuffle = true, ulong? seed = null)
        => new(PolarsWrapper.SampleFrac(Handle, fraction, withReplacement, shuffle, seed));

    // ==========================================
    // Combining DataFrames
    // ==========================================
    /// <summary>
    /// Join with another DataFrame
    /// </summary>
    /// <param name="other"></param>
    /// <param name="leftOn"></param>
    /// <param name="rightOn"></param>
    /// <param name="how"></param>
    /// <returns></returns>
    public DataFrame Join(DataFrame other, Expr[] leftOn, Expr[] rightOn, JoinType how = JoinType.Inner)
    {
        var lHandles = leftOn.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        var rHandles = rightOn.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();

    return new DataFrame(PolarsWrapper.Join(
            Handle, 
            other.Handle, 
            lHandles, 
            rHandles, 
            how.ToNative()
        ));
    }
    /// <summary>
    /// Join with another DataFrame using column names.
    /// </summary>
    public DataFrame Join(DataFrame other, string[] leftOn, string[] rightOn, JoinType how = JoinType.Inner)
    {
        var lExprs = leftOn.Select(c => Polars.Col(c)).ToArray();
        var rExprs = rightOn.Select(c => Polars.Col(c)).ToArray();

        return Join(other, lExprs, rExprs, how);
    }

    /// <summary>
    /// Join with another DataFrame using a single column pair.
    /// </summary>
    public DataFrame Join(DataFrame other, string leftOn, string rightOn, JoinType how = JoinType.Inner)
        => Join(other, [leftOn], [rightOn], how);
    /// <summary>
    /// Perform an As-Of Join (time-series join).
    /// <para>
    /// This is an Eager operation that executes via the Lazy engine.
    /// The keys must be sorted.
    /// </para>
    /// </summary>
    public DataFrame JoinAsOf(
        DataFrame other, 
        Expr leftOn, Expr rightOn, 
        string? tolerance = null,
        string strategy = "backward",
        Expr[]? leftBy = null,
        Expr[]? rightBy = null)
    {
        return this.Lazy()
            .JoinAsOf(
                other.Lazy(), 
                leftOn, 
                rightOn, 
                tolerance, 
                strategy, 
                leftBy, 
                rightBy
            )
            .Collect();
    }

    /// <summary>
    /// Perform an As-Of Join with tolerance as timespan (time-series join).
    /// </summary>
    public DataFrame JoinAsOf(
        DataFrame other, 
        Expr leftOn, Expr rightOn, 
        TimeSpan tolerance,
        string strategy = "backward",
        Expr[]? leftBy = null,
        Expr[]? rightBy = null)
    {
        return JoinAsOf(
            other,
            leftOn,
            rightOn,
            DurationFormatter.ToPolarsString(tolerance),
            strategy,
            leftBy,
            rightBy
        );
    }
    /// <summary>
    /// Concatenate multiple DataFrames
    /// </summary>
    /// <param name="dfs"></param>
    /// <param name="how"></param>
    /// <returns></returns>
    public static DataFrame Concat(IEnumerable<DataFrame> dfs, ConcatType how = ConcatType.Vertical)
    {
        var handles = dfs.Select(d => PolarsWrapper.CloneDataFrame(d.Handle)).ToArray();
        
        return new DataFrame(PolarsWrapper.Concat(handles, how.ToNative()));
    }

    // ==========================================
    // GroupBy
    // ==========================================
    /// <summary>
    /// Group by keys and apply aggregations.
    /// </summary>
    /// <param name="by"></param>
    /// <returns></returns>
    public GroupByBuilder GroupBy(params Expr[] by) => new (this, by);
    /// <summary>
    /// Group by column names.
    /// </summary>
    public GroupByBuilder GroupBy(params string[] columns)
    {
        var exprs = columns.Select(Polars.Col).ToArray();
        return GroupBy(exprs);
    }
    /// <summary>
    /// Group by dynamic windows based on a time index.
    /// <para>
    /// This is an Eager operation that executes via the Lazy engine for performance.
    /// </para>
    /// </summary>
    public DynamicGroupBy GroupByDynamic(
        string indexColumn,
        TimeSpan every,
        TimeSpan? period = null,
        TimeSpan? offset = null,
        Expr[]? by = null,
        Label label = Label.Left,
        bool includeBoundaries = false,
        ClosedWindow closedWindow = ClosedWindow.Left,
        StartBy startBy = StartBy.WindowBound
    )
    {
        return new DynamicGroupBy(
            this,
            indexColumn,
            every,
            period,
            offset,
            by,
            label,
            includeBoundaries,
            closedWindow,
            startBy
        );
    }

    // ==========================================
    // Pivot / Unpivot
    // ==========================================
    /// <summary>
    /// Pivot the DataFrame from long to wide format.
    /// </summary>
    /// <param name="index"></param>
    /// <param name="columns"></param>
    /// <param name="values"></param>
    /// <param name="agg"></param>
    /// <returns></returns>
    public DataFrame Pivot(string[] index, string[] columns, string[] values, PivotAgg agg = PivotAgg.First)
        => new(PolarsWrapper.Pivot(Handle, index, columns, values, agg.ToNative()));
    /// <summary>
    /// Unpivot (Melt) the DataFrame from wide to long format.
    /// </summary>
    /// <param name="index"></param>
    /// <param name="on"></param>
    /// <param name="variableName"></param>
    /// <param name="valueName"></param>
    /// <returns></returns>
    public DataFrame Unpivot(string[] index, string[] on, string variableName = "variable", string valueName = "value")
        => new(PolarsWrapper.Unpivot(Handle, index, on, variableName, valueName));
    /// <summary>
    /// Unpivot (Melt) the DataFrame from wide to long format.
    /// </summary>
    /// <param name="index"></param>
    /// <param name="on"></param>
    /// <param name="variableName"></param>
    /// <param name="valueName"></param>
    /// <returns></returns>
    public DataFrame Melt(string[] index, string[] on, string variableName = "variable", string valueName = "value") 
        => Unpivot(index, on, variableName, valueName);

    // ==========================================
    // IO Write
    // ==========================================
    /// <summary>
    /// Write DataFrame to CSV File
    /// </summary>
    /// <param name="path"></param>
    public void WriteCsv(string path)
        => PolarsWrapper.WriteCsv(Handle, path);
    /// <summary>
    /// Write DataFrame to Parquet File
    /// </summary>
    /// <param name="path"></param>
    public void WriteParquet(string path)
        => PolarsWrapper.WriteParquet(Handle, path);
    /// <summary>
    /// Write DataFrame to IPC File    
    /// </summary>
    /// <param name="path"></param>
    public void WriteIpc(string path)
        => PolarsWrapper.WriteIpc(Handle, path);
    /// <summary>
    /// Write DataFrame to JSON File
    /// </summary>
    /// <param name="path"></param>
    public void WriteJson(string path)
        => PolarsWrapper.WriteJson(Handle, path);
    /// <summary>
    /// Export DataFrame to Record Batch
    /// </summary>
    /// <param name="onBatchReceived">Receive RecordBatch Callback</param>
    public void ExportBatches(Action<RecordBatch> onBatchReceived)
        => PolarsWrapper.ExportBatches(Handle, onBatchReceived);
    /// <summary>
    /// Common Write Interface:Transform DataFrame to IDataReader
    /// </summary>
    public void WriteTo( Action<IDataReader> writerAction, int bufferSize = 5,Dictionary<string, Type>? typeOverrides = null)
    {
        using var buffer = new BlockingCollection<RecordBatch>(bufferSize);

        // Consumer Task
        var consumerTask = Task.Run(() => 
        {
            using var reader = new ArrowToDbStream(buffer.GetConsumingEnumerable(),typeOverrides = null);
            
            writerAction(reader);
        });

        // Producer
        try
        {
            ExportBatches(buffer.Add);
        }
        finally
        {
            buffer.CompleteAdding();
        }

        consumerTask.Wait();
    }
    /// <summary>
    /// Generate a summary statistics DataFrame (count, mean, std, min, 25%, 50%, 75%, max).
    /// Similar to pandas/polars describe().
    /// </summary>
    public DataFrame Describe()
    {
        // 1. Select Numeric Column
        var schema = Schema;
        var numericCols = schema
            .Where(kv => kv.Value.IsNumeric)
            .Select(kv => kv.Key)
            .ToList();

        if (numericCols.Count == 0)
            throw new InvalidOperationException("No numeric columns to describe.");

        // 2. Define stastistical metrics
        var metrics = new List<(string Name, Func<string, Expr> Op)>
        {
            ("count",      c => Polars.Col(c).Count().Cast(DataType.Float64)),
            ("null_count", c => Polars.Col(c).IsNull().Sum().Cast(DataType.Float64)),
            ("mean",       c => Polars.Col(c).Mean()),
            ("std",        c => Polars.Col(c).Std()),
            ("min",        c => Polars.Col(c).Min().Cast(DataType.Float64)),
            ("25%",        c => Polars.Col(c).Quantile(0.25, "nearest").Cast(DataType.Float64)),
            ("50%",        c => Polars.Col(c).Median().Cast(DataType.Float64)),
            ("75%",        c => Polars.Col(c).Quantile(0.75, "nearest").Cast(DataType.Float64)),
            ("max",        c => Polars.Col(c).Max().Cast(DataType.Float64))
        };

        var rowFrames = new List<DataFrame>();
        
        try
        {
            foreach (var (statName, op) in metrics)
            {
                var exprs = new List<Expr>
                {
                    Polars.Lit(statName).Alias("statistic")
                };

                foreach (var col in numericCols)
                {
                    exprs.Add(op(col));
                }

                rowFrames.Add(Select(exprs.ToArray()));
            }

            return Concat(rowFrames);
        }
        finally
        {
            foreach (var frame in rowFrames)
            {
                frame.Dispose();
            }
        }
    }
    // ==========================================
    // Display (Show)
    // ==========================================
    /// <summary>
    /// Returns the string representation of the DataFrame (ASCII table).
    /// This allows Console.WriteLine(df) to print the table directly.
    /// </summary>
    public override string ToString()
    {
        if (Handle.IsInvalid) return "DataFrame (Disposed)";
        return PolarsWrapper.DataFrameToString(Handle);
    }

    /// <summary>
    /// Print the DataFrame to Console.
    /// </summary>
    public void Show() => Console.WriteLine(ToString());
    // ==========================================
    // Interop
    // ==========================================

    /// <summary>
    /// Clone the DataFrame
    /// </summary>
    /// <returns></returns>
    public DataFrame Clone()
        => new(PolarsWrapper.CloneDataFrame(Handle));
    /// <summary>
    /// Dispose the DataFrame and release resources.
    /// </summary>
    public void Dispose() => Handle?.Dispose();
    // ==========================================
    // Object Mapping (From Records)
    // ==========================================

    /// <summary>
    /// Create a DataFrame from a collection of objects (POCOs).
    /// High-performance implementation using Arrow Struct conversion.
    /// </summary>
    public static DataFrame From<T>(IEnumerable<T> data)
    {
        using var structSeries = Series.From("data", data);
        
        using var tmpDf = new DataFrame(structSeries);
        
        return tmpDf.Unnest("data");
    }
    /// <summary>
    /// Create a DataFrame from an object where properties represent columns (Arrays/Lists).
    /// This is useful for "Structure of Arrays" (SoA) data layout.
    /// </summary>
    /// <example>
    /// var df = DataFrame.FromColumns(new { 
    ///     Time = new[] { dt1, dt2 }, 
    ///     Val = new[] { 1.0, 2.0 } 
    /// });
    /// </example>
    public static DataFrame FromColumns(object columns)
    {
        var rawColumns = ArrowConverter.BuildColumnsFromObject(columns);

        var seriesList = new List<Series>(rawColumns.Count);
        foreach (var (name, array) in rawColumns)
        {
            seriesList.Add(Series.FromArrow(name, array));
        }

        return new DataFrame(seriesList.ToArray());
    }
    /// <summary>
    /// Create a DataFrame from a list of Series.
    /// </summary>
    public DataFrame(params Series[] series)
    {
        if (series == null || series.Length == 0)
        {
            Handle = PolarsWrapper.DataFrameNew(System.Array.Empty<SeriesHandle>());
            return;
        }

        var handles = series.Select(s => s.Handle).ToArray();
        
        Handle = PolarsWrapper.DataFrameNew(handles);
    }
    /// <summary>
    /// [High Performance] Stream data into Polars using Arrow C Stream Interface.
    /// This method supports datasets larger than available RAM by streaming chunks directly to Polars.
    /// </summary>
    /// <param name="data">Source data collection</param>
    /// <param name="batchSize">Rows per chunk (default 100,000)</param>
    public static DataFrame FromArrowStream<T>(IEnumerable<T> data, int batchSize = 100_000)
    {
        var stream = data.ToArrowBatches(batchSize);

        var handle = ArrowStreamInterop.ImportEager(stream);

        if (handle.IsInvalid)
        {
            return From(Enumerable.Empty<T>());
        }

        return new DataFrame(handle);
    }
    
    // ==========================================
    // Object Mapping (To Records)
    // ==========================================

    /// <summary>
    /// Convert DataFrame to a list of strongly-typed objects.
    /// This triggers a conversion to Arrow format internally.
    /// </summary>
    public IEnumerable<T> Rows<T>() where T : new()
    {
        using var batch = ToArrow(); 

        foreach (var item in ArrowReader.ReadRecordBatch<T>(batch))
        {
            yield return item;
        }
    }

    // ==========================================
    // Conversion to Lazy
    // ==========================================

    /// <summary>
    /// Convert the DataFrame into a LazyFrame.
    /// This allows building a query plan and optimizing execution.
    /// </summary>
    public LazyFrame Lazy()
    {
        var clonedHandle = PolarsWrapper.CloneDataFrame(Handle);
        
        var lfHandle = PolarsWrapper.DataFrameToLazy(clonedHandle);
        
        return new LazyFrame(lfHandle);
    }
    // ==========================================
    // Series Access (Column Selection)
    // ==========================================

    /// <summary>
    /// Get a column as a Series by name.
    /// </summary>
    public Series Column(string name)
    {
        var sHandle = PolarsWrapper.DataFrameGetColumn(Handle, name);
        
        return new Series(name, sHandle);
    }

    /// <summary>
    /// Get a column as a Series by name (Indexer syntax).
    /// Usage: var s = df["age"];
    /// </summary>
    public Series this[string columnName]
    {
        get => Column(columnName);
    }

    /// <exception cref="IndexOutOfRangeException"></exception>
    /// <summary>
    /// Get a column by its positional index (0-based).
    /// </summary>
    public Series Column(int index)
    {
        var h = PolarsWrapper.DataFrameGetColumnAt(Handle, index);
        return new Series(h);
    }
    /// <summary>
    /// Get all columns as a list of Series.
    /// Order is guaranteed to match the physical column order.
    /// </summary>
    public Series[] GetColumns()
    {
        var names = PolarsWrapper.GetColumnNames(Handle);
        
        var cols = new Series[names.Length];
        for (int i = 0; i < names.Length; i++)
        {
            cols[i] = Column(names[i]);
        }
        return cols;
    }
    
    /// <summary>
    /// Indexer to get a column by position.
    /// Usage: var s = df[0];
    /// </summary>
    public Series this[int index] => Column(index);
    /// <summary>
    /// Syntax Suger
    /// </summary>
    /// <param name="rowIndex"></param>
    /// <param name="columnIndex"></param>
    /// <returns></returns>
    public object? this[int rowIndex, int columnIndex]
    {
        get
        {
            var series = Column(columnIndex);
            return series[rowIndex];
        }
    }
    /// <summary>
    /// Get data foir selected row.
    /// </summary>
    public object?[] Row(int index)
    {
        if (index < 0 || index >= Height)
            throw new IndexOutOfRangeException($"Row index {index} is out of bounds. Height: {Height}");

        var rowData = new object?[Width];
        for (int i = 0; i < Width; i++)
        {
            rowData[i] = this[index, i];
        }
        return rowData;
    }
    /// <summary>
    /// Enable foreach (var series in df) { ... }
    /// </summary>
    /// <returns></returns>
    public IEnumerator<Series> GetEnumerator()
    {
        for (int i = 0; i < Width; i++)
        {
            yield return Column(i);
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    /// <summary>
    /// Generates an HTML representation of the DataFrame.
    /// Useful for rendering in Jupyter/Polyglot Notebooks.
    /// </summary>
    /// <param name="limit">Max rows to display (default 10).</param>
    public string ToHtml(int limit = 10)
    {
        var sb = new StringBuilder();
        
        // Basic Style
        sb.Append(@"
<style>
.pl-dataframe { font-family: sans-serif; border-collapse: collapse; width: auto; }
.pl-dataframe th { background-color: #f0f0f0; color: #333; font-weight: bold; text-align: left; padding: 8px; border-bottom: 2px solid #ddd; }
.pl-dataframe td { padding: 8px; border-bottom: 1px solid #ddd; text-align: left; color: #444; }
.pl-dataframe tr:hover { background-color: #f9f9f9; }
.pl-dtype { font-size: 0.8em; color: #888; display: block; font-weight: normal; }
.pl-null { color: #d66; font-style: italic; }
.pl-dim { font-size: 0.8em; color: #666; margin-top: 5px; }
</style>");

        sb.Append("<table class='pl-dataframe'>");

        // Table Header
        sb.Append("<thead><tr>");
        
        for (int i = 0; i < Width; i++)
        {
            var col = Column(i);
            var colName = System.Net.WebUtility.HtmlEncode(col.Name);
            var colType = col.DataType.ToString();
            
            sb.Append($"<th>{colName}<span class='pl-dtype'>{colType}</span></th>");
        }
        sb.Append("</tr></thead>");

        // Body
        sb.Append("<tbody>");
        
        int rowsToShow = (int)Math.Min(Height, limit);
        for (int r = 0; r < rowsToShow; r++)
        {
            sb.Append("<tr>");
            var rowData = Row(r);
            
            foreach (var val in rowData)
            {
                if (val == null || val == DBNull.Value)
                {
                    sb.Append("<td class='pl-null'>null</td>");
                }
                else
                {
                    string cellStr = val switch
                    {
                        DateTime dt => dt.ToString("yyyy-MM-dd HH:mm:ss"),
                        double d => d.ToString("G6"), 
                        _ => val.ToString() ?? ""
                    };
                    
                    if (cellStr.Length > 50) cellStr = cellStr[..47] + "...";
                    
                    sb.Append($"<td>{System.Net.WebUtility.HtmlEncode(cellStr)}</td>");
                }
            }
            sb.Append("</tr>");
        }
        sb.Append("</tbody>");
        sb.Append("</table>");

        // Shape
        var hiddenRows = Height - rowsToShow;
        if (hiddenRows > 0)
        {
            sb.Append($"<div class='pl-dim'>... and {hiddenRows} more rows.</div>");
        }
        sb.Append($"<div class='pl-dim'>shape: ({Height}, {Width})</div>");

        return sb.ToString();
    }
}