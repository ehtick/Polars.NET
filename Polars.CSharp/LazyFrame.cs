using System.Collections.Concurrent;
using System.Data;
using Apache.Arrow;
using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Data;

namespace Polars.CSharp;

/// <summary>
/// Represents a lazily evaluated DataFrame.
/// Until the query is executed, operations are just recorded in a query plan.
/// Once executed, the data is materialized in memory.
/// </summary>
public class LazyFrame : IDisposable
{
    internal LazyFrameHandle Handle { get; }

    internal LazyFrame(LazyFrameHandle handle)
    {
        Handle = handle;
    }

    // ==========================================
    // Scan IO
    // ==========================================
    /// <summary>
    /// Scans a CSV file lazily.
    /// </summary>
    public static LazyFrame ScanCsv(
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

        var handle = PolarsWrapper.ScanCsv(
            path, 
            schemaHandles, 
            hasHeader, 
            separator, 
            skipRows,
            tryParseDates
        );

        return new LazyFrame(handle);
    }
    /// <summary>
    /// Read a Parquet file as a LazyFrame.
    /// </summary>
    /// <param name="path"></param>
    /// <returns></returns>
    public static LazyFrame ScanParquet(string path)
        =>  new(PolarsWrapper.ScanParquet(path));
    /// <summary>
    /// Read an IPC (Feather) file as a LazyFrame.
    /// </summary>
    /// <param name="path"></param>
    /// <returns></returns>
    public static LazyFrame ScanIpc(string path)
        => new(PolarsWrapper.ScanIpc(path));
    /// <summary>
    /// Read a NDJSON file as a LazyFrame.
    /// </summary>
    /// <param name="path"></param>
    /// <returns></returns>
    public static LazyFrame ScanNdjson(string path) 
        => new(PolarsWrapper.ScanNdjson(path));

    private static IEnumerable<RecordBatch> EnsureStreamSafety(IEnumerable<RecordBatch> source)
    {
        using var enumerator = source.GetEnumerator();

        while (enumerator.MoveNext())
        {
            var batch = enumerator.Current;
            yield return batch;
            batch.Dispose();
        }
    }

    /// <summary>
    /// Scan Enumberable As LazyFrame
    /// </summary>
    /// <returns></returns>
    public static LazyFrame ScanEnumerable<T>(
        IEnumerable<T> data, 
        Schema? schema = null, 
        int batchSize = 100_000,
        bool useBuffered = false)
    {
        // 1. Get Schema (Cached)
        schema ??= ArrowConverter.GetSchemaFromType<T>();

        // 2. Buffered Mode
        if (useBuffered)
        {
            var scope = new IpcStreamService.TempIpcScope<T>(data, batchSize); 
            var handleBuffered = PolarsWrapper.ScanIpc(scope.FilePath!);
            return new ScopedLazyFrame(handleBuffered, scope);
        }

        // 3. Streaming Mode (Memory Pointer)

        IEnumerable<RecordBatch> SafeGenerator()
        {
            bool hasYielded = false;

            foreach (var batch in data.ToArrowBatches(batchSize))
            {
                hasYielded = true;
                yield return batch;
            }

            if (!hasYielded)
            {
                yield return ArrowConverter.GetEmptyBatch<T>();
            }
        }

        var handle = ArrowStreamInterop.ScanStream(
            () => EnsureStreamSafety(SafeGenerator()), 
            schema
        );
        
        return new LazyFrame(handle);
    }

    /// <summary>
    /// Scan RecordBatch Stream
    /// If schema is provied, first batch won't be consumed for getting schema.
    /// </summary>
    public static LazyFrame ScanRecordBatches(IEnumerable<RecordBatch> stream, Schema schema)
        {
            if (schema == null) throw new ArgumentNullException(nameof(schema));

            var handle = ArrowStreamInterop.ScanStream(
                () => EnsureStreamSafety(stream),
                schema
            );
            return new LazyFrame(handle);
        }
    /// <summary>
    /// Scan Database to LazyFrame
    /// </summary>
    /// <param name="reader"></param>
    /// <param name="batchSize"></param>
    /// <returns></returns>
    public static LazyFrame ScanDatabase(IDataReader reader, int batchSize = 50_000)
    {
        var schema = reader.GetArrowSchema();
        
        var stream = reader.ToArrowBatches(batchSize);

        return ScanRecordBatches(stream, schema);
    }
    /// <summary>
    /// Create a LazyFrame from a database query using a reader factory.
    /// <para>
    /// <b>Recommended:</b> This is the preferred method for interacting with databases in a Lazy context.
    /// </para>
    /// <para>
    /// It accepts a factory function that creates a NEW <see cref="IDataReader"/> on demand.
    /// This allows Polars to:
    /// <list type="bullet">
    /// <item>Inspect the schema upfront (using a probe reader).</item>
    /// <item>Re-execute the query if the execution plan requires multiple passes.</item>
    /// <item>Allow you to call <see cref="Collect"/> multiple times on the same LazyFrame.</item>
    /// </list>
    /// </para>
    /// </summary>
    /// <param name="readerFactory">A function that returns a new, open <see cref="IDataReader"/> instance each time it is called.</param>
    /// <param name="batchSize">The size of the Arrow record batch (rows). Larger values reduce overhead.</param>
    /// <returns>A new LazyFrame linked to the database stream.</returns>
    /// <example>
    /// <code>
    /// // Define a factory that returns a new Reader for the query
    /// Func&lt;IDataReader&gt; factory = () =>
    /// {
    ///     var cmd = connection.CreateCommand();
    ///     cmd.CommandText = """
    ///         SELECT name, score
    ///         FROM User
    ///         WHERE score > $min_score OR score IS NULL
    ///     """;
    ///     cmd.Parameters.AddWithValue("$min_score", 60.0);
    ///     return cmd.ExecuteReader();
    /// };
    /// 
    /// // Scan and apply transformations lazily
    /// var lf = LazyFrame.ScanDatabase(factory);
    /// 
    /// var result = lf
    ///     .WithColumns(
    ///         Col("score").FillNull(0.0).Alias("clean_score")
    ///     )
    ///     .Collect();
    ///     
    /// result.Show();
    /// /* Output:
    /// shape: (3, 3)
    /// ┌─────────┬───────┬─────────────┐
    /// │ name    ┆ score ┆ clean_score │
    /// │ ---     ┆ ---   ┆ ---         │
    /// │ str     ┆ f64   ┆ f64         │
    /// ╞═════════╪═══════╪═════════════╡
    /// │ Alice   ┆ 99.5  ┆ 99.5        │
    /// │ Bob     ┆ 85.0  ┆ 85.0        │
    /// │ Charlie ┆ null  ┆ 0.0         │
    /// └─────────┴───────┴─────────────┘
    /// */
    /// </code>
    /// </example>
    public static LazyFrame ScanDatabase(Func<IDataReader> readerFactory, int batchSize = 50_000)
        {
            Schema schema;
            // Probe schema
            using (var probe = readerFactory())
            {
                schema = probe.GetArrowSchema();
            }

            // Replayable stream
            IEnumerable<RecordBatch> StreamFactory()
            {
                using var reader = readerFactory();
                foreach (var batch in reader.ToArrowBatches(batchSize))
                    yield return batch;
            }

            var handle = ArrowStreamInterop.ScanStream(
                () => EnsureStreamSafety(StreamFactory()),
                schema
            );
            return new LazyFrame(handle);
        }
    /// <summary>
    /// A LazyFrame with resource scope which needs to be disposed.
    /// 当它被 Dispose 时，除了释放 LazyFrame 句柄，还会清理关联的临时资源（如磁盘上的 IPC 文件）。
    /// </summary>
    public class ScopedLazyFrame : LazyFrame
    {
        private readonly IDisposable? _resource;

        // 🟢 修正：构造函数接收 LazyFrameHandle 以匹配基类
        internal ScopedLazyFrame(LazyFrameHandle handle, IDisposable? resource) 
            : base(handle) 
        {
            _resource = resource;
        }
        /// <summary>
        /// Dispose temp file and lazyframe
        /// </summary>
        public new void Dispose()
        {
            // 1. 先释放基类持有的 Rust 句柄
            base.Dispose();
            
            // 2. 再清理 C# 端的临时资源 (例如删除临时文件)
            _resource?.Dispose();
        }
    }
    /// <summary>
    /// [Buffered] Create a LazyFrame from an existing DataReader.
    /// <para><b>Note:</b> This consumes the reader IMMEDIATELY and writes to a temp file.</para>
    /// <para>Returns a <see cref="ScopedLazyFrame"/> which must be disposed to delete the temp file.</para>
    /// </summary>
    public static ScopedLazyFrame ScanDatabaseBuffered(IDataReader reader, int batchSize = 50_000)
    {
        // DataReader cannot be reset, so we must buffer it to disk immediately
        var scope = new IpcStreamService.TempIpcScopeReader(reader, batchSize);
        
        var handle = PolarsWrapper.ScanIpc(scope.FilePath!);
        
        return new ScopedLazyFrame(handle, scope);
    }
    // ==========================================
    // Meta / Inspection
    // ==========================================

    /// <summary>
    /// Fetch the schema as a dictionary of column names and their data types.
    /// </summary>
    /// <summary>
    /// Gets the Schema of the LazyFrame.
    /// <para>
    /// This operation triggers type inference on the query plan.
    /// The returned DataTypes are strongly typed and backed by native handles.
    /// </para>
    /// </summary>
    public Dictionary<string, DataType> Schema
    {
        get
        {
            using var schemaHandle = PolarsWrapper.GetLazySchema(Handle);
            
            ulong len = PolarsWrapper.GetSchemaLen(schemaHandle);
            
            var result = new Dictionary<string, DataType>((int)len);

            for (ulong i = 0; i < len; i++)
            {
                PolarsWrapper.GetSchemaFieldAt(schemaHandle, i, out string name, out DataTypeHandle dtHandle);
                
                result[name] = new DataType(dtHandle);
            }

            return result;
        }
    }

    /// <summary>
    /// Get Schema description string
    /// </summary>
    public string SchemaString
    {
        get
        {
            var schema = this.Schema;

            // Format: {"a": i32, "b": list[str], "c": datetime[ms]}
            var parts = schema.Select(kv => $"\"{kv.Key}\": {kv.Value}");
            
            return "{" + string.Join(", ", parts) + "}";
        }
    }

    /// <summary>
    /// Get an explanation of the optimized query plan.
    /// <para>
    /// Returns a string representation of the logical plan after Polars optimizers 
    /// (predicate pushdown, projection pushdown, etc.) have run.
    /// </para>
    /// </summary>
    /// <param name="optimized">If true, show the optimized plan. If false, show the logical plan as built.</param>
    /// <returns>The plan as a string.</returns>
    /// <example>
    /// <code>
    /// var q = df.Lazy()
    ///     .Filter(Col("group") != "C")
    ///     .WithColumns((Col("val") * 2).Alias("val_x_2"))
    ///     .Select("group", "val_x_2");
    /// 
    /// Console.WriteLine(q.Explain());
    /// /* Output (Optimized Plan):
    /// simple π 2/2 ["group", "val_x_2"]
    ///    WITH_COLUMNS:
    ///    [[(col("val")) * (2)].alias("val_x_2")] 
    ///     FILTER [(col("group")) != ("C")]
    ///     FROM
    ///       DF ["group", "val"]; PROJECT["group", "val"] 2/2 COLUMNS
    /// */
    /// </code>
    /// </example>
    public string Explain(bool optimized = true)
    {
        return PolarsWrapper.Explain(Handle, optimized);
    }
    /// <summary>
    /// Clone the LazyFrame, creating a new independent copy.
    /// </summary>
    /// <returns></returns>
    public LazyFrame Clone()
    {
        return new LazyFrame(PolarsWrapper.LazyClone(Handle));
    }
    internal LazyFrameHandle CloneHandle()
    {
        return PolarsWrapper.LazyClone(Handle);
    }
    // ==========================================
    // Transformations
    // ==========================================
    /// <summary>
    /// Select specific columns or expressions.
    /// </summary>
    /// <example>
    /// <code>
    /// // Select "a" and calculate "b" * 2
    /// lf.Select(Col("a"), (Col("b") * 2).Alias("b_double"));
    /// </code>
    /// </example>
    public LazyFrame Select(params Expr[] exprs)
    {
        var lfClone = CloneHandle();
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        return new LazyFrame(PolarsWrapper.LazySelect(lfClone, handles));
    }
    /// <summary>
    /// Select columns by name.
    /// <para>Syntactic sugar for <c>Select(Expr.Col(name))</c>.</para>
    /// </summary>
    public LazyFrame Select(params string[] columns)
    {
        var exprs = columns.Select(Polars.Col).ToArray();
        return Select(exprs);
    }
    /// <summary>
    /// Filter rows based on a boolean expression.
    /// <para>
    /// In a LazyFrame, this operation is added to the logical plan and is optimized before execution.
    /// Polars will attempt to push this filter down as close to the data source as possible (Predicate Pushdown).
    /// </para>
    /// </summary>
    /// <param name="expr">A boolean expression.</param>
    /// <returns>A new LazyFrame with the filter applied.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     group = new[] { "A", "A", "B", "B", "C" },
    ///     val = new[] { 1, 2, 3, 4, 5 }
    /// });
    /// 
    /// // Build a lazy query:
    /// // 1. Filter out group 'C'
    /// // 2. Multiply 'val' by 2
    /// // 3. Select specific columns
    /// var q = df.Lazy()
    ///     .Filter(Col("group") != "C")
    ///     .WithColumns((Col("val") * 2).Alias("val_x_2"))
    ///     .Select("group", "val_x_2");
    /// 
    /// // Execute
    /// q.Collect().Show();
    /// /* Output:
    /// shape: (4, 2)
    /// ┌───────┬─────────┐
    /// │ group ┆ val_x_2 │
    /// │ ---   ┆ ---     │
    /// │ str   ┆ i32     │
    /// ╞═══════╪═════════╡
    /// │ A     ┆ 2       │
    /// │ A     ┆ 4       │
    /// │ B     ┆ 6       │
    /// │ B     ┆ 8       │
    /// └───────┴─────────┘
    /// */
    /// </code>
    /// </example>
    public LazyFrame Filter(Expr expr)
    {
        var lfClone = CloneHandle();
        var h = PolarsWrapper.CloneExpr(expr.Handle);
        //
        return new LazyFrame(PolarsWrapper.LazyFilter(lfClone, h));
    }
    /// <summary>
    /// Add or modify columns based on expressions.
    /// </summary>
    /// <example>
    /// <code>
    /// // Add a new column "c" while keeping "a" and "b"
    /// lf.WithColumns((Col("a") + Col("b")).Alias("c"));
    /// </code>
    /// </example>
    public LazyFrame WithColumns(params Expr[] exprs)
    {
        var lfClone = CloneHandle();
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        return new LazyFrame(PolarsWrapper.LazyWithColumns(lfClone, handles));
    }
    /// <summary>
    /// Slice the LazyFrame.
    /// <para>This operation is lazy; it only affects the query plan.</para>
    /// </summary>
    /// <param name="offset">Start index. Negative values count from the end.</param>
    /// <param name="length">Number of rows to return.</param>
    public LazyFrame Slice(long offset, uint length)
    {
        var handle = PolarsWrapper.LazySlice(CloneHandle(), offset, length);
        return new LazyFrame(handle);
    }
    /// <summary>
    /// Slice the LazyFrame (Convenience overload).
    /// </summary>
    public LazyFrame Slice(long offset, int length)
    {
        if (length < 0) throw new ArgumentOutOfRangeException(nameof(length), "Length must be non-negative.");
        return Slice(offset, (uint)length);
    }
    /// <summary>
    /// Sort the LazyFrame by a single column.
    /// </summary>
    public LazyFrame Sort(
        string column, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
        => Sort([Polars.Col(column)], [descending], [nullsLast], maintainOrder);
    /// <summary>
    /// Sort using a single expression.
    /// </summary>
    public LazyFrame Sort(
        Expr expr, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
    {
        return Sort([expr], [descending], [nullsLast], maintainOrder);
    }
    /// <summary>
    /// Sort using multiple exprs and single option.
    /// </summary>
    /// <param name="exprs"></param>
    /// <param name="descending"></param>
    /// <param name="nullsLast"></param>
    /// <param name="maintainOrder"></param>
    /// <returns></returns>
    public LazyFrame Sort(
        Expr[] exprs, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
    {
        return Sort(exprs, [descending], [nullsLast], maintainOrder);
    }
    /// <summary>
    /// Sort the LazyFrame by multiple columns (all ascending or all descending).
    /// </summary>
    public LazyFrame Sort(
        string[] columns, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
    {
        var exprs = columns.Select(Polars.Col).ToArray();
        return Sort(exprs, [descending], [nullsLast], maintainOrder);
    }

    /// <summary>
    /// Lazily sort the DataFrame by multiple columns.
    /// <para>
    /// This operation is added to the logical plan. 
    /// Use <see cref="TopK(int, string, bool)"/> if you only need the top/bottom N rows, as it is more efficient.
    /// </para>
    /// </summary>
    /// <param name="columns">Names of the columns to sort by.</param>
    /// <param name="descending">Sort order for each column.</param>
    /// <param name="nullsLast">Whether nulls go last for each column.</param>
    /// <param name="maintainOrder">Whether to maintain the relative order of rows with equal keys.</param>
    /// <seealso cref="DataFrame.Sort(string[], bool[], bool[], bool)"/>
    /// <example>
    /// <code>
    /// df.Lazy()
    ///   .Sort(
    ///       columns: new[] { "group", "val" }, 
    ///       descending: new[] { false, true }, 
    ///       nullsLast: new[] { false, false }
    ///   )
    ///   .Collect();
    /// /* Output:
    /// shape: (5, 2)
    /// ┌───────┬─────┐
    /// │ group ┆ val │
    /// │ ---   ┆ --- │
    /// │ str   ┆ i32 │
    /// ╞═══════╪═════╡
    /// │ A     ┆ 10  │
    /// │ A     ┆ 8   │
    /// │ ...   ┆ ... │
    /// └───────┴─────┘
    /// */
    /// </code>
    /// </example>
    public LazyFrame Sort(
        string[] columns, 
        bool[] descending, 
        bool[] nullsLast, 
        bool maintainOrder = false)
    {
        var exprs = columns.Select(Polars.Col).ToArray();
        return Sort(exprs, descending, nullsLast, maintainOrder);
    }

    /// <summary>
    /// Sort the LazyFrame by multiple exprs.
    /// </summary>
    public LazyFrame Sort(
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

        var h = PolarsWrapper.LazyFrameSort(
            Handle, 
            clonedHandles, 
            descending, 
            nullsLast, 
            maintainOrder
        );
        
        return new LazyFrame(h);
    }
    /// <summary>
    /// Get the top k rows according to the given expressions.
    /// <para>This selects the largest values.</para>
    /// </summary>
    /// <param name="k">Number of rows to return.</param>
    /// <param name="by">Expressions to sort by.</param>
    /// <param name="reverse">
    /// If true, select the smallest values (reverse the sort order) for that column.
    /// </param>
    public LazyFrame TopK(int k, Expr[] by, bool[] reverse)
    {
        if (by.Length != reverse.Length)
            throw new ArgumentException("Length of 'by' and 'reverse' must match.");

        var lfHandle = CloneHandle(); // Consume self
        var clonedHandles = new ExprHandle[by.Length];
        for (int i = 0; i < by.Length; i++)
        {
            clonedHandles[i] = PolarsWrapper.CloneExpr(by[i].Handle);
        }

        var h = PolarsWrapper.LazyFrameTopK(lfHandle, (uint)k, clonedHandles, reverse);
        return new LazyFrame(h);
    }

    /// <summary>
    /// Get the top k rows according to a single expression.
    /// </summary>
    public LazyFrame TopK(int k, Expr by, bool reverse = false)
    {
        return TopK(k, [by], [reverse]);
    }
    
    /// <summary>
    /// Get the top k rows according to a single column name.
    /// </summary>
    /// <param name="k"></param>
    /// <param name="colName"></param>
    /// <param name="reverse"></param>
    /// <returns></returns>
    public LazyFrame TopK(int k, string colName, bool reverse = false)
    {
        return TopK(k, Polars.Col(colName), reverse);
    }

    /// <summary>
    /// Get the bottom k rows according to the given expressions.
    /// <para>This selects the smallest values.</para>
    /// </summary>
    public LazyFrame BottomK(int k, Expr[] by, bool[] reverse)
    {
        if (by.Length != reverse.Length)
            throw new ArgumentException("Length of 'by' and 'reverse' must match.");

        var lfHandle = CloneHandle();
        var clonedHandles = new ExprHandle[by.Length];
        for (int i = 0; i < by.Length; i++)
        {
            clonedHandles[i] = PolarsWrapper.CloneExpr(by[i].Handle);
        }

        var h = PolarsWrapper.LazyFrameBottomK(lfHandle, (uint)k, clonedHandles, reverse);
        return new LazyFrame(h);
    }

    /// <summary>
    /// Get the bottom k rows according to a single expression.
    /// </summary>
    public LazyFrame BottomK(int k, Expr by, bool reverse = false)
    {
        return BottomK(k, [by], [reverse]);
    }
    /// <summary>
    /// Get the bottom k rows according to a single column name.
    /// </summary>
    /// <param name="k"></param>
    /// <param name="colName"></param>
    /// <param name="reverse"></param>
    /// <returns></returns>
    public LazyFrame BottomK(int k, string colName, bool reverse = false)
    {
        return BottomK(k, Polars.Col(colName), reverse);
    }
    /// <summary>
    /// Limit the number of rows in the LazyFrame.
    /// </summary>
    /// <param name="n"></param>
    /// <returns></returns>
    public LazyFrame Limit(uint n)
    {
        var lfClone = CloneHandle();
        return new LazyFrame(PolarsWrapper.LazyLimit(lfClone, n));
    }
    /// <summary>
    /// Lazily unnest struct columns.
    /// <para>
    /// Currently uses a Selector to perform the unnesting.
    /// </para>
    /// </summary>
    /// <seealso cref="DataFrame.Unnest(string[], string?)"/>
    /// <example>
    /// <code>
    /// df.Lazy()
    ///   .Unnest("User")
    ///   .Collect();
    /// </code>
    /// </example>
    public LazyFrame Unnest(Selector selector,string? separator = null)
    {
        var lfClone = CloneHandle();
        var sClone = selector.CloneHandle();
        var h = PolarsWrapper.LazyFrameUnnest(lfClone, sClone,separator);
        return new LazyFrame(h);
    }

    /// <summary>
    /// Unnest specific struct columns by name.
    /// (Syntactic sugar for Unnest(Selector.Cols(...)))
    /// </summary>
    public LazyFrame Unnest(params string[] columns)
    {
        using var sel = Selector.Cols(columns);
        return Unnest(sel,null);
    }
    /// <summary>
    /// Explode list-like columns into multiple rows.
    /// </summary>
    /// <param name="exprs"></param>
    /// <returns></returns>
    public LazyFrame Explode(params Expr[] exprs)
    {
        var lfClone = CloneHandle();
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();

        return new LazyFrame(PolarsWrapper.LazyExplode(lfClone, handles));
    }

    // ==========================================
    // Reshaping
    // ==========================================
    /// <summary>
    /// Unpivot (Melt) the DataFrame from wide to long format.
    /// </summary>
    /// <param name="index"></param>
    /// <param name="on"></param>
    /// <param name="variableName"></param>
    /// <param name="valueName"></param>
    /// <returns></returns>
    public LazyFrame Unpivot(string[] index, string[] on, string variableName = "variable", string valueName = "value")
    {
        var lfClone = CloneHandle();
        return new LazyFrame(PolarsWrapper.LazyUnpivot(lfClone, index, on, variableName, valueName));
    }
    /// <summary>
    /// Melt the DataFrame from wide to long format.
    /// </summary>
    /// <param name="index"></param>
    /// <param name="on"></param>
    /// <param name="variableName"></param>
    /// <param name="valueName"></param>
    /// <returns></returns>
    public LazyFrame Melt(string[] index, string[] on, string variableName = "variable", string valueName = "value") 
        => Unpivot(index, on, variableName, valueName);
    /// <summary>
    /// Lazily concatenate multiple LazyFrames.
    /// <para>
    /// This adds a concat node to the query plan. 
    /// For vertical concatenation, schemas must align (or be capable of supertype unification).
    /// </para>
    /// </summary>
    /// <example>
    /// <code>
    /// LazyFrame.Concat(new[] { lf1, lf2 }, ConcatType.Vertical)
    ///          .Collect();
    /// </code>
    /// </example>
    public static LazyFrame Concat(
        IEnumerable<LazyFrame> lfs, 
        ConcatType how = ConcatType.Vertical, 
        bool rechunk = false, 
        bool parallel = true)
    {
        var lfClones = lfs.Select(l => l.CloneHandle()).ToArray();
        var handles = lfClones.Select(l => l).ToArray();
        return new LazyFrame(PolarsWrapper.LazyConcat(handles, how.ToNative(), rechunk, parallel));
    }

    // ==========================================
    // Join
    // ==========================================
    /// <summary>
    /// Lazily join with another LazyFrame.
    /// <para>
    /// Polars will optimize the join execution order. 
    /// Note: Both frames must be LazyFrames.
    /// </para>
    /// </summary>
    /// <seealso cref="DataFrame.Join(DataFrame, Expr[], Expr[], JoinType)"/>
    /// <example>
    /// <code>
    /// var lf1 = df1.Lazy();
    /// var lf2 = df2.Lazy();
    /// 
    /// // Lazy Left Join
    /// var joined = lf1.Join(lf2, Col("id"), Col("id"), JoinType.Left)
    ///                 .Collect();
    ///                 
    /// /* Output:
    /// shape: (3, 3)
    /// ┌─────┬─────────┬───────┐
    /// │ id  ┆ name    ┆ score │
    /// │ --- ┆ ---     ┆ ---   │
    /// │ i32 ┆ str     ┆ i32   │
    /// ╞═════╪═════════╪═══════╡
    /// │ 1   ┆ Alice   ┆ 90    │
    /// │ 2   ┆ Bob     ┆ 80    │
    /// │ 3   ┆ Charlie ┆ null  │
    /// └─────┴─────────┴───────┘
    /// */
    /// </code>
    /// </example>
    public LazyFrame Join(LazyFrame other, Expr[] leftOn, Expr[] rightOn, JoinType how = JoinType.Inner)
    {
        var lOn = leftOn.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        var rOn = rightOn.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        var lfClone = CloneHandle();
        var otherClone = other.CloneHandle();
        return new LazyFrame(PolarsWrapper.Join(
            lfClone, 
            otherClone, 
            lOn, 
            rOn, 
            how.ToNative()
        ));
    }
    /// <summary>
    /// Join with another LazyFrame on a single column.
    /// </summary>
    /// <param name="other"></param>
    /// <param name="leftOn"></param>
    /// <param name="rightOn"></param>
    /// <param name="how"></param>
    /// <returns></returns>
    public LazyFrame Join(LazyFrame other, Expr leftOn, Expr rightOn, JoinType how = JoinType.Inner)
    {
        return Join(other,[leftOn], [rightOn], how);
    }
    /// <summary>
    /// Join with another LazyFrame using column names.
    /// </summary>
    public LazyFrame Join(LazyFrame other, string[] leftOn, string[] rightOn, JoinType how = JoinType.Inner)
    {
        var lExprs = leftOn.Select(Polars.Col).ToArray();
        var rExprs = rightOn.Select(Polars.Col).ToArray();

        return Join(other, lExprs, rExprs, how);
    }

    /// <summary>
    /// Join with another LazyFrame using a single column pair.
    /// </summary>
    public LazyFrame Join(LazyFrame other, string leftOn, string rightOn, JoinType how = JoinType.Inner)
        => Join(other, [leftOn], [rightOn], how);

    /// <summary>
    /// Perform an As-Of Join (time-series join).
    /// <para>
    /// This is similar to a left-join except that we match on nearest key rather than equal keys.
    /// The keys must be sorted.
    /// </para>
    /// </summary>
    /// <param name="other">The LazyFrame to join with.</param>
    /// <param name="leftOn">Join key of the left LazyFrame.</param>
    /// <param name="rightOn">Join key of the right LazyFrame.</param>
    /// <param name="tolerance">
    /// Numeric tolerance (e.g. "2", "-1") or temporal tolerance (e.g. "2h", "10s").
    /// Matches that are further away than this tolerance are discarded.
    /// </param>
    /// <param name="strategy">
    /// The strategy to determine which value is "nearest".
    /// <list type="bullet">
    /// <item>
    ///     <term>backward</term>
    ///     <description>(Default) Search for the last row in the right frame where <c>right_on &lt;= left_on</c>. 
    ///     Equivalent to "last known value".</description>
    /// </item>
    /// <item>
    ///     <term>forward</term>
    ///     <description>Search for the first row in the right frame where <c>right_on &gt;= left_on</c>.
    ///     Equivalent to "next available value".</description>
    /// </item>
    /// <item>
    ///     <term>nearest</term>
    ///     <description>Search for the row in the right frame where the absolute difference <c>|left_on - right_on|</c> is smallest.</description>
    /// </item>
    /// </list>
    /// </param>
    /// <param name="leftBy">Join on these columns exactly (equivalence join) before performing as-of join.</param>
    /// <param name="rightBy">Join on these columns exactly (equivalence join) before performing as-of join.</param>
    public LazyFrame JoinAsOf(
        LazyFrame other, 
        Expr leftOn, Expr rightOn, 
        string? tolerance = null,
        string strategy = "backward",
        Expr[]? leftBy = null,
        Expr[]? rightBy = null)
    {
        var lfClone = CloneHandle();
        var otherClone = other.CloneHandle();
        var lOn = PolarsWrapper.CloneExpr(leftOn.Handle);
        var rOn = PolarsWrapper.CloneExpr(rightOn.Handle);
        
        var lBy = leftBy?.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        var rBy = rightBy?.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();

        return new LazyFrame(PolarsWrapper.JoinAsOf(
            lfClone, otherClone,
            lOn, rOn,
            lBy, rBy,
            strategy, tolerance
        ));
    }
    /// <summary>
    /// <inheritdoc cref="JoinAsOf(LazyFrame, Expr, Expr, string, string, Expr[], Expr[])" path="/summary"/>
    /// </summary>
    /// <param name="other"><inheritdoc cref="JoinAsOf(LazyFrame, Expr, Expr, string, string, Expr[], Expr[])" path="/param[@name='other']"/></param>
    /// <param name="leftOn"><inheritdoc cref="JoinAsOf(LazyFrame, Expr, Expr, string, string, Expr[], Expr[])" path="/param[@name='leftOn']"/></param>
    /// <param name="rightOn"><inheritdoc cref="JoinAsOf(LazyFrame, Expr, Expr, string, string, Expr[], Expr[])" path="/param[@name='rightOn']"/></param>
    /// <param name="tolerance">
    /// The temporal tolerance as a <see cref="TimeSpan"/>. 
    /// Matches that are further away than this duration are discarded.
    /// </param>
    /// <param name="strategy"><inheritdoc cref="JoinAsOf(LazyFrame, Expr, Expr, string, string, Expr[], Expr[])" path="/param[@name='strategy']"/></param>
    /// <param name="leftBy"><inheritdoc cref="JoinAsOf(LazyFrame, Expr, Expr, string, string, Expr[], Expr[])" path="/param[@name='leftBy']"/></param>
    /// <param name="rightBy"><inheritdoc cref="JoinAsOf(LazyFrame, Expr, Expr, string, string, Expr[], Expr[])" path="/param[@name='rightBy']"/></param>
    /// <returns><inheritdoc cref="JoinAsOf(LazyFrame, Expr, Expr, string, string, Expr[], Expr[])" path="/returns"/></returns>
    public LazyFrame JoinAsOf(
        LazyFrame other, 
        Expr leftOn, Expr rightOn, 
        TimeSpan tolerance,
        string strategy = "backward",
        Expr[]? leftBy = null,
        Expr[]? rightBy = null)
    {
        return JoinAsOf(other,leftOn,rightOn,DurationFormatter.ToPolarsString(tolerance),strategy,leftBy,rightBy);
    }
    // ==========================================
    // GroupBy
    // ==========================================
    /// <summary>
    /// Start a lazy GroupBy operation.
    /// </summary>
    /// <remarks>
    /// Unlike <see cref="DataFrame.GroupBy(Expr[])"/> which returns a <see cref="GroupByBuilder"/>,
    /// this returns a <see cref="LazyGroupBy"/> object which allows constructing the aggregation plan.
    /// </remarks>
    /// <example>
    /// <code>
    /// df.Lazy()
    ///   .GroupBy("group")
    ///   .Agg(Col("val").Sum().Alias("sum_val"))
    ///   .Collect();
    ///   
    /// /* Output:
    /// shape: (2, 2)
    /// ┌───────┬─────────┐
    /// │ group ┆ sum_val │
    /// │ ---   ┆ ---     │
    /// │ str   ┆ i32     │
    /// ╞═══════╪═════════╡
    /// │ A     ┆ 3       │
    /// │ B     ┆ 7       │
    /// └───────┴─────────┘
    /// */
    /// </code>
    /// </example>
    public LazyGroupBy GroupBy(params Expr[] keys)
    {
        var lfClone = CloneHandle();
        
        return new LazyGroupBy(lfClone, keys);
    }
    /// <summary>
    /// Group by a single column name.
    /// <para>
    /// Explicit overload to ensure the string is treated as a Column, not a Literal.
    /// </para>
    /// </summary>
    public LazyGroupBy GroupBy(string name)
        => GroupBy([Polars.Col(name)]);

    /// <summary>
    /// Group by multiple column names.
    /// <para>
    /// Explicit overload to ensure strings are treated as Columns, not Literals.
    /// </para>
    /// </summary>
    public LazyGroupBy GroupBy(params string[] names)
    {
        var exprs = names.Select(n => Polars.Col(n)).ToArray();
        return GroupBy(exprs);
    }
    /// <summary>
    /// Lazily group based on a time index using dynamic windows.
    /// <para>
    /// This defines a dynamic groupby in the query plan.
    /// </para>
    /// </summary>
    /// <seealso cref="DataFrame.GroupByDynamic"/>
    /// <example>
    /// <code>
    /// df.Lazy()
    ///   .GroupByDynamic("time", every: TimeSpan.FromHours(1))
    ///   .Agg(Col("val").Sum().Alias("total"))
    ///   .Collect();
    /// </code>
    /// </example>
    public LazyDynamicGroupBy GroupByDynamic(
        string indexColumn,
        TimeSpan every,
        TimeSpan? period = null,
        TimeSpan? offset = null,
        Expr[]? by = null,
        Label label = Label.Left, // [修改] 默认 Left
        bool includeBoundaries = false,
        ClosedWindow closedWindow = ClosedWindow.Left,
        StartBy startBy = StartBy.WindowBound
    )
    {
        string everyStr = DurationFormatter.ToPolarsString(every);
        string periodStr = DurationFormatter.ToPolarsString(period) ?? everyStr;
        string offsetStr = DurationFormatter.ToPolarsString(offset) ?? "0s";

        var keys = by ?? [];
        return new LazyDynamicGroupBy(
            CloneHandle(),
            indexColumn,
            everyStr,
            periodStr,
            offsetStr,
            keys,
            label, 
            includeBoundaries,
            closedWindow,
            startBy
        );
    }
    // ==========================================
    // Execution (Collect)
    // ==========================================

    /// <summary>
    /// Execute the query plan and return a DataFrame.
    /// </summary>
    public DataFrame Collect()
        => new(PolarsWrapper.LazyCollect(Handle));

    /// <summary>
    /// Execute the query plan using the streaming engine.
    /// </summary>
    public DataFrame CollectStreaming()
        => new(PolarsWrapper.CollectStreaming(Handle));
    /// <summary>
    /// Execute the query plan asynchronously and return a DataFrame.
    /// </summary>
    public async Task<DataFrame> CollectAsync()
    {
        var dfHandle = await PolarsWrapper.LazyCollectAsync(Handle);
        return new DataFrame(dfHandle);
    }
    // ==========================================
    // Output Sink (IO)
    // ==========================================
    /// <summary>
    /// Sink the LazyFrame to a Parquet file.
    /// </summary>
    /// <param name="path"></param>
    public void SinkParquet(string path)
        => PolarsWrapper.SinkParquet(Handle, path);
    /// <summary>
    /// Sink the LazyFrame to a CSV file.
    /// </summary>
    /// <param name="path"></param>
    public void SinkIpc(string path)
       => PolarsWrapper.SinkIpc(Handle, path);
    /// <summary>
    /// Sink the LazyFrame to JSON file.
    /// </summary>
    /// <param name="path"></param>
    public void SinkJson(string path)
        => PolarsWrapper.SinkJson(Handle, path);
    /// <summary>
    /// Sink the LazyFrame to CSV file.
    /// </summary>
    /// <param name="path"></param>
    public void SinkCsv(string path)
        => PolarsWrapper.SinkCsv(Handle, path);
    /// <summary>
    /// Streaming Sink to Batchs
    /// </summary>
    public void SinkBatches(Action<RecordBatch> onBatchReceived)
    {
        using var newLfHandle = PolarsWrapper.SinkBatches(CloneHandle(), onBatchReceived);

        using var lfRes = new LazyFrame(newLfHandle);
        using var _ = lfRes.CollectStreaming(); 
    }
    /// <summary>
    /// Stream the result of the LazyFrame calculation into an <see cref="IDataReader"/>.
    /// <para>
    /// This allows processing huge datasets that don't fit in memory by handling them in chunks (RecordBatches).
    /// </para>
    /// <para>
    /// Common use cases:
    /// <list type="bullet">
    /// <item>Bulk inserting data into SQL Databases (using SqlBulkCopy or NpgsqlBinaryImporter).</item>
    /// <item>Streaming data to other .NET libraries that consume IDataReader.</item>
    /// </list>
    /// </para>
    /// </summary>
    /// <param name="writerAction">
    /// A callback that receives the <see cref="IDataReader"/>. 
    /// This action executes on a separate thread (Consumer) while the Polars engine (Producer) pumps data.
    /// </param>
    /// <param name="bufferSize">
    /// The number of RecordBatches to buffer in memory. 
    /// If the buffer is full, the Polars engine will pause until the consumer reads more data (Backpressure).
    /// </param>
    /// <param name="typeOverrides">Optional schema overrides to guide the type mapping.</param>
    /// <example>
    /// <code>
    /// // Simulate a large lazy computation
    /// var lf = DataFrame.FromColumns(new { id = new[] { 0, 1, 2, 3, 4 } }).Lazy();
    /// 
    /// // Stream result to a database writer (simulated here)
    /// lf.SinkTo(reader => 
    /// {
    ///     Console.WriteLine("[DB Writer] Started receiving data...");
    ///     while (reader.Read())
    ///     {
    ///         var val = reader.GetValue(0);
    ///         Console.WriteLine($"[DB Writer] Insert row: {val}");
    ///     }
    ///     Console.WriteLine("[DB Writer] Done.");
    /// }, bufferSize: 2);
    /// 
    /// /* Output:
    /// [DB Writer] Started receiving data...
    /// [DB Writer] Insert row: 0
    /// [DB Writer] Insert row: 1
    /// ...
    /// [DB Writer] Done.
    /// */
    /// </code>
    /// </example>
    public void SinkTo(Action<IDataReader> writerAction, int bufferSize = 5,Dictionary<string, Type>? typeOverrides = null)
    {
        // 1. Producer-Consumer buffer
        using var buffer = new BlockingCollection<RecordBatch>(boundedCapacity: bufferSize);

        // 2. Start consumer (DB Writer)
        var consumerTask = Task.Run(() => 
        {
            // ArrowToDbStream is responsible for disguising Buffer as DataReader
            // It automatically handles Dispose, so Batch will be released after writerAction finishes reading
            using var reader = new ArrowToDbStream(buffer.GetConsumingEnumerable(),typeOverrides);
            
            // Hand over the reader to user logic
            // Users call bulk.WriteToServer(reader) here
            writerAction(reader);
        });

        // 3. Start producer (Polars Engine - blocking execution in current thread)
        try
        {
            // Push data produced by Rust into Buffer
            // If Buffer is full, this will block, thereby automatically backpressuring the Rust engine
            SinkBatches(buffer.Add);
        }
        finally
        {
            // 4. Notify consumer: no more data
            buffer.CompleteAdding();
        }

        // 5. Wait for consumer to finish writing and throw possible exceptions
        consumerTask.Wait();
    }
    /// <summary>
    /// Dispose the LazyFrame and release native resources.
    /// </summary>
    public void Dispose() => Handle?.Dispose();
}