#pragma warning disable CS1573

using System.Collections.Concurrent;
using System.Data;
using Apache.Arrow;
using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Data;
using Polars.NET.Core.Helpers;

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
    /// Lazily scans a CSV file into a LazyFrame.
    /// </summary>
    /// <param name="path">Path to the CSV file.</param>
    /// <param name="schema">Optional PolarsSchema to specify types or overwrite schema.</param>
    /// <param name="hasHeader">Whether the CSV has a header row.</param>
    /// <param name="separator">Character used as separator.</param>
    /// <param name="ignoreErrors">Whether to ignore parsing errors (skip bad rows).</param>
    /// <param name="tryParseDates">Whether to automatically try parsing dates/datetimes.</param>
    /// <param name="lowMemory">Reduce memory usage at the cost of performance.</param>
    /// <param name="cache">Cache the result after reading (default true).</param>
    /// <param name="rechunk">Rechunk the memory to contiguous chunks after reading (default false).</param>
    /// <param name="skipRows">Number of rows to skip at start.</param>
    /// <param name="nRows">Stop reading after n rows.</param>
    /// <param name="inferSchemaLength">Number of rows to scan for schema inference (100 is default).</param>
    /// <param name="rowIndexName">If provided, adds a row index column with this name.</param>
    /// <param name="rowIndexOffset">Offset to start the row index from.</param>
    /// <param name="encoding">File encoding (UTF8 or LossyUTF8).</param>
    /// <returns>A new LazyFrame.</returns>
    public static LazyFrame ScanCsv(
        string path,
        PolarsSchema? schema = null, // ✅ 升级为强类型 Schema 对象
        bool hasHeader = true,
        char separator = ',',
        bool ignoreErrors = false,
        bool tryParseDates = true,
        bool lowMemory = false,
        bool cache = true,           // ✅ Lazy 特有参数
        bool rechunk = false,        // ✅ Lazy 特有参数
        ulong skipRows = 0,
        ulong? nRows = null,
        ulong? inferSchemaLength = 100,
        string? rowIndexName = null, // ✅ 行号生成
        ulong rowIndexOffset = 0,
        PlEncoding encoding = PlEncoding.UTF8)
    {
        var handle = PolarsWrapper.ScanCsv(
            path,
            schema?.Handle,
            hasHeader,
            separator,
            ignoreErrors,
            tryParseDates,
            lowMemory,
            cache,
            rechunk,
            skipRows,
            nRows,
            inferSchemaLength,
            rowIndexName,
            rowIndexOffset,
            encoding.ToNative()
        );

        return new LazyFrame(handle);
    }
    /// <summary>
    /// Lazily scans a CSV from an in-memory byte array.
    /// Useful for processing data from Web APIs, S3, or embedded resources without writing to disk.
    /// </summary>
    public static LazyFrame ScanCsv(
        byte[] buffer, 
        PolarsSchema? schema = null,
        bool hasHeader = true,
        char separator = ',',
        bool ignoreErrors = false,
        bool tryParseDates = true,
        bool lowMemory = false,
        bool cache = true,
        bool rechunk = false,
        ulong skipRows = 0,
        ulong? nRows = null,
        ulong? inferSchemaLength = 100,
        string? rowIndexName = null,
        ulong rowIndexOffset = 0,
        PlEncoding encoding = PlEncoding.UTF8)
    {
        // 调用 Wrapper
        var handle = PolarsWrapper.ScanCsv(
            buffer,
            schema?.Handle,
            hasHeader,
            separator,
            ignoreErrors,
            tryParseDates,
            lowMemory,
            cache,
            rechunk,
            skipRows,
            nRows,
            inferSchemaLength,
            rowIndexName,
            rowIndexOffset,
            encoding.ToNative()
        );

        return new LazyFrame(handle);
    }
    /// <summary>
    /// Lazily read from a parquet file or multiple files via glob patterns.
    /// </summary>
    /// <param name="path">Path to file or glob pattern (e.g. "data/*.parquet").</param>
    /// <param name="nRows">Limit number of rows to read (optimization).</param>
    /// <param name="parallel">Parallel strategy.</param>
    /// <param name="lowMemory">Reduce memory usage at the expense of performance.</param>
    /// <param name="useStatistics">Use parquet statistics to optimize the query plan.</param>
    /// <param name="glob">Expand glob patterns (default: true).</param>
    /// <param name="allowMissingColumns">Allow missing columns when reading multiple files.</param>
    /// <param name="rowIndexName">If provided, adds a column with the row number.</param>
    /// <param name="rowIndexOffset">Offset for the row index.</param>
    /// <param name="includePathColumn">If provided, adds a column with the source file path.</param>
    /// <param name="schema">
    /// Manually specify the schema of the file(s). 
    /// Useful if the file footer is missing or to avoid I/O overhead of reading the schema.
    /// </param>
    /// <param name="hivePartitionSchema">
    /// Manually specify the schema for Hive partitioning columns.
    /// Use this to ensure specific types for partition keys (e.g. string instead of int).
    /// </param>
    /// <param name="tryParseHiveDates">
    /// Whether to try parsing dates in Hive partitioning paths (default: true).
    /// </param>
    public static LazyFrame ScanParquet(
        string path,
        ulong? nRows = null,
        ParallelStrategy parallel = ParallelStrategy.Auto,
        bool lowMemory = false,
        bool useStatistics = true,
        bool glob = true,
        bool allowMissingColumns = false,
        string? rowIndexName = null,
        uint rowIndexOffset = 0,
        string? includePathColumn = null,
        PolarsSchema? schema = null,
        PolarsSchema? hivePartitionSchema = null,
        bool tryParseHiveDates = false)
    {
        var schemaHandle = schema?.Handle;
        var hiveSchemaHandle = hivePartitionSchema?.Handle;

        var h = PolarsWrapper.ScanParquet(
            path,
            nRows,
            parallel.ToNative(),
            lowMemory,
            useStatistics,
            glob,
            allowMissingColumns,
            rowIndexName,
            rowIndexOffset,
            includePathColumn,
            schemaHandle,     
            hiveSchemaHandle, 
            tryParseHiveDates
        );

        return new LazyFrame(h);
    }
    /// <summary>
    /// Lazily read parquet from an in-memory byte array.
    /// </summary>
    public static LazyFrame ScanParquet(
        byte[] buffer,
        ulong? nRows = null,
        ParallelStrategy parallel = ParallelStrategy.Auto,
        bool lowMemory = false,
        bool useStatistics = true,
        bool allowMissingColumns = false,
        string? rowIndexName = null,
        uint rowIndexOffset = 0,
        string? includePathColumn = null,
        PolarsSchema? schema = null,
        PolarsSchema? hivePartitionSchema = null,
        bool tryParseHiveDates = false)
    {
        var schemaHandle = schema?.Handle;
        var hiveSchemaHandle = hivePartitionSchema?.Handle;

        var h = PolarsWrapper.ScanParquet(
            buffer,
            nRows,
            parallel.ToNative(),
            lowMemory,
            useStatistics,
            false, // glob = false for memory
            allowMissingColumns,
            rowIndexName,
            rowIndexOffset,
            includePathColumn,
            schemaHandle,
            hiveSchemaHandle,
            tryParseHiveDates
        );

        return new LazyFrame(h);
    }
    // ---------------------------------------------------------
    // Scan IPC (File)
    // ---------------------------------------------------------

    /// <summary>
    /// Lazily read an Arrow IPC (Feather v2) file.
    /// </summary>
    /// <param name="path">Path to the IPC file.</param>
    /// <param name="schema">
    /// Optional schema to enforce. If not provided, the schema is inferred from the file footer.
    /// </param>
    /// <param name="nRows">
    /// Limit the number of rows to scan. 
    /// Note: In Lazy mode, this acts as a 'Pre-Slice' pushdown.
    /// </param>
    /// <param name="rechunk">Rechunk the memory to be contiguous (default: false).</param>
    /// <param name="cache">Cache the result of the scan (default: true).</param>
    /// <param name="rowIndexName">If provided, adds a column with the row index.</param>
    /// <param name="rowIndexOffset">Offset for the row index (default: 0).</param>
    /// <param name="includePathColumn">If provided, adds a column with the source file path.</param>
    /// <param name="hivePartitioning">Enable Hive partitioning inference (default: false).</param>
    public static LazyFrame ScanIpc(
        string path,
        PolarsSchema? schema = null,
        ulong? nRows = null,
        bool rechunk = false,
        bool cache = true,
        string? rowIndexName = null,
        uint rowIndexOffset = 0,
        string? includePathColumn = null,
        bool hivePartitioning = false)
    {
        if (!File.Exists(path)) 
            throw new FileNotFoundException($"IPC file not found: {path}");

        var schemaHandle = schema?.Handle;

        var h = PolarsWrapper.ScanIpc(
            path,
            schemaHandle,
            nRows,
            rechunk,
            cache,
            rowIndexName,
            rowIndexOffset,
            includePathColumn,
            hivePartitioning
        );

        return new LazyFrame(h);
    }

    // ---------------------------------------------------------
    // Scan IPC (Memory / Bytes)
    // ---------------------------------------------------------

    /// <summary>
    /// Lazily read Arrow IPC (Feather v2) from in-memory bytes.
    /// </summary>
    public static LazyFrame ScanIpc(
        byte[] buffer,
        PolarsSchema? schema = null,
        ulong? nRows = null,
        bool rechunk = false,
        bool cache = true,
        string? rowIndexName = null,
        uint rowIndexOffset = 0,
        bool hivePartitioning = false)
    {
        var schemaHandle = schema?.Handle;

        var h = PolarsWrapper.ScanIpc(
            buffer,
            schemaHandle,
            nRows,
            rechunk,
            cache,
            rowIndexName,
            rowIndexOffset,
            hivePartitioning
        );

        return new LazyFrame(h);
    }

    // ---------------------------------------------------------
    // Scan IPC (Stream)
    // ---------------------------------------------------------

    /// <summary>
    /// Lazily read Arrow IPC (Feather v2) from a Stream.
    /// </summary>
    /// <remarks>
    /// This reads the stream fully into memory to construct the Lazy execution plan.
    /// </remarks>
    public static LazyFrame ScanIpc(
        Stream stream,
        PolarsSchema? schema = null,
        ulong? nRows = null,
        bool rechunk = false,
        bool cache = true,
        string? rowIndexName = null,
        uint rowIndexOffset = 0,
        bool hivePartitioning = false)
    {
        // 必须读入内存，因为 ScanSources 需要持有数据所有权
        using var ms = new MemoryStream();
        stream.CopyTo(ms);
        
        return ScanIpc(
            ms.ToArray(),
            schema,
            nRows,
            rechunk,
            cache,
            rowIndexName,
            rowIndexOffset,
            hivePartitioning
        );
    }
    // ---------------------------------------------------------
    // Scan NDJSON (File)
    // ---------------------------------------------------------

    /// <summary>
    /// Lazily read a newline delimited JSON file (NDJSON).
    /// </summary>
    /// <param name="path">Path to the NDJSON file.</param>
    /// <param name="schema">
    /// Manually specify schema for specific columns (Overwrite semantics).
    /// Columns not specified will be inferred.
    /// </param>
    /// <param name="inferSchemaLength">
    /// Number of rows to scan for schema inference. 
    /// If null, uses Polars default (usually 100).
    /// </param>
    /// <param name="batchSize">Batch size for reading (optimization).</param>
    /// <param name="nRows">Limit the number of rows to read.</param>
    /// <param name="lowMemory">Reduce memory usage at the expense of performance.</param>
    /// <param name="rechunk">Rechunk the output to have contiguous memory (default: false).</param>
    /// <param name="ignoreErrors">Ignore parsing errors (skip malformed lines).</param>
    /// <param name="rowIndexName">If provided, adds a column with the row index.</param>
    /// <param name="rowIndexOffset">Offset for the row index (default: 0).</param>
    /// <param name="includePathColumn">If provided, adds a column with the source file path.</param>
    public static LazyFrame ScanNdjson(
        string path,
        PolarsSchema? schema = null,
        ulong? inferSchemaLength = null,
        ulong? batchSize = null,
        ulong? nRows = null,
        bool lowMemory = false,
        bool rechunk = false,
        bool ignoreErrors = false,
        string? rowIndexName = null,
        uint rowIndexOffset = 0,
        string? includePathColumn = null)
    {
        if (!File.Exists(path)) 
            throw new FileNotFoundException($"NDJSON file not found: {path}");

        var schemaHandle = schema?.Handle;

        var h = PolarsWrapper.ScanNdjson(
            path,
            schemaHandle,
            batchSize,
            inferSchemaLength,
            nRows,
            lowMemory,
            rechunk,
            ignoreErrors,
            rowIndexName,
            rowIndexOffset,
            includePathColumn
        );

        return new LazyFrame(h);
    }

    // ---------------------------------------------------------
    // Scan NDJSON (Memory / Bytes)
    // ---------------------------------------------------------

    /// <summary>
    /// Lazily read NDJSON from an in-memory byte array.
    /// </summary>
    public static LazyFrame ScanNdjson(
        byte[] buffer,
        PolarsSchema? schema = null,
        ulong? inferSchemaLength = null,
        ulong? batchSize = null,
        ulong? nRows = null,
        bool lowMemory = false,
        bool rechunk = false,
        bool ignoreErrors = false,
        string? rowIndexName = null,
        uint rowIndexOffset = 0)
    {
        var schemaHandle = schema?.Handle;

        var h = PolarsWrapper.ScanNdjson(
            buffer,
            schemaHandle,
            batchSize,
            inferSchemaLength,
            nRows,
            lowMemory,
            rechunk,
            ignoreErrors,
            rowIndexName,
            rowIndexOffset,
            null // includePathColumn
        );

        return new LazyFrame(h);
    }

    // ---------------------------------------------------------
    // Scan NDJSON (Stream)
    // ---------------------------------------------------------

    /// <summary>
    /// Lazily read NDJSON from a Stream.
    /// </summary>
    public static LazyFrame ScanNdjson(
        Stream stream,
        PolarsSchema? schema = null,
        ulong? inferSchemaLength = null,
        ulong? batchSize = null,
        ulong? nRows = null,
        bool lowMemory = false,
        bool rechunk = false,
        bool ignoreErrors = false,
        string? rowIndexName = null,
        uint rowIndexOffset = 0)
    {
        using var ms = new MemoryStream();
        stream.CopyTo(ms);
        
        return ScanNdjson(
            ms.ToArray(),
            schema,
            inferSchemaLength,
            batchSize,
            nRows,
            lowMemory,
            rechunk,
            ignoreErrors,
            rowIndexName,
            rowIndexOffset
        );
    }

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
        Apache.Arrow.Schema? schema = null, 
        int batchSize = 100_000,
        bool useBuffered = false)
    {
        // 1. Get Schema (Cached)
        schema ??= ArrowConverter.GetSchemaFromType<T>();

        // 2. Buffered Mode
        if (useBuffered)
        {
            var scope = new IpcStreamService.TempIpcScope<T>(data, batchSize); 
            var handleBuffered = ScanIpc(scope.FilePath!).Handle;
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
    public static LazyFrame ScanRecordBatches(IEnumerable<RecordBatch> stream, Apache.Arrow.Schema schema)
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
            Apache.Arrow.Schema schema;
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
    /// </summary>
    public class ScopedLazyFrame : LazyFrame
    {
        private readonly IDisposable? _resource;

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
            base.Dispose();
            
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
        
        var handle = ScanIpc(scope.FilePath!).Handle;
        
        return new ScopedLazyFrame(handle, scope);
    }
    // ==========================================
    // Meta / Inspection
    // ==========================================

    /// <summary>
    /// Gets the Schema of the LazyFrame.
    /// Note: The returned PolarsSchema object is IDisposable. 
    /// Usage in 'using' block is recommended if accessed frequently.
    /// </summary>
    public PolarsSchema Schema
    {
        get
        {
            var handle = PolarsWrapper.GetLazySchema(Handle);
            
            return new PolarsSchema(handle);
        }
    }

    /// <summary>
    /// Prints the schema to the console.
    /// </summary>
    public void PrintSchema()
    {
        using var schema = Schema;
        
        Console.WriteLine("root");
        
        foreach (var name in schema.ColumnNames)
        {
            var type = schema[name]; 
            Console.WriteLine($" |-- {name}: {type.Kind}");
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
    /// Drop selected columns by selector.
    /// </summary>
    /// <param name="selector"></param>
    /// <returns></returns>
    public LazyFrame Drop(Selector selector)
    {
        var lfClone = CloneHandle();
        var sClone = selector.CloneHandle();
        var h = PolarsWrapper.LazyFrameDrop(lfClone, sClone);
        return new LazyFrame(h);
    }
    /// <summary>
    /// Drop selected columns by column names.
    /// </summary>
    /// <param name="columns"></param>
    /// <returns></returns>
    public LazyFrame Drop(params string[] columns)
    {
        using var sel = Selector.Cols(columns);
        return Drop(sel);
    }
    /// <summary>
    /// Keep unique rows (stable) based on a subset of columns defined by a Selector.
    /// </summary>
    /// <param name="subset">Selector defining the subset of columns. If null, uses all columns.</param>
    /// <param name="keep">Strategy to keep duplicates (First, Last, Any, None).</param>
    public LazyFrame Unique(Selector? subset = null, UniqueKeepStrategy keep = UniqueKeepStrategy.First)
    {
        _ = subset?.CloneHandle(); // Clone handle or pass reference depending on ownership model

        var h = PolarsWrapper.LazyUniqueStable(CloneHandle(), subset?.Handle!, keep.ToNative());
        return new LazyFrame(h);
    }
    /// <summary>
    /// Keep unique rows (stable) based on specific column names.
    /// </summary>
    public LazyFrame Unique(UniqueKeepStrategy keep, params string[] columns)
    {
        using var sel = Selector.Cols(columns);
        return Unique(sel, keep);
    }
    /// <summary>
    /// Keep unique First rows (stable) based on a subset of columns defined column names.
    /// </summary>
    /// <param name="columns"></param>
    /// <returns></returns>
    public LazyFrame Unique(params string[] columns)
    {
        return Unique(UniqueKeepStrategy.First, columns);
    }
    /// <summary>
    /// Explode list-like columns into multiple rows.
    /// </summary>
    /// <param name="selector"></param>
    /// <returns></returns>
    public LazyFrame Explode(Selector selector)
    {
        var lfClone = CloneHandle();
        var sClone = selector.CloneHandle();
        var h = PolarsWrapper.LazyExplode(lfClone, sClone);
        return new LazyFrame(h);
    }
    /// <summary>
    /// Explode list-like columns into multiple rows.
    /// </summary>
    /// <param name="columns"></param>
    /// <returns></returns>
    public LazyFrame Explode(params string[] columns)
    {
        using var sel = Selector.Cols(columns);
        return Explode(sel);
    }

    // ==========================================
    // Reshaping
    // ==========================================
    /// <summary>
    /// Unpivot (Melt) the LazyFrame from wide to long format.
    /// </summary>
    /// <param name="index"></param>
    /// <param name="on"></param>
    /// <param name="variableName"></param>
    /// <param name="valueName"></param>
    /// <returns></returns>
    public LazyFrame Unpivot(Selector index, Selector on, string variableName = "variable", string valueName = "value")
    {
        var lfClone = CloneHandle();
        var indexClone = index.CloneHandle();
        var onClone = on.CloneHandle();
        return new LazyFrame(PolarsWrapper.LazyUnpivot(lfClone, indexClone, onClone, variableName, valueName));
    }
    /// <summary>
    /// Unpivot using column names (String Array overload).
    /// Wraps strings into Selectors automatically.
    /// </summary>
    public LazyFrame Unpivot(string[] index, string[] on, string variableName = "variable", string valueName = "value")
    {
        using var sIndex = Selector.Cols(index);
        using var sOn = Selector.Cols(on);

        return Unpivot(sIndex, sOn, variableName, valueName);
    }
    /// <summary>
    /// Unpivot using single column names (Convenience overload).
    /// </summary>
    public LazyFrame Unpivot(string index, string on, string variableName = "variable", string valueName = "value")
    {
        return Unpivot([index], [on], variableName, valueName);
    }
    /// <summary>
    /// Melt the DataFrame from wide to long format.
    /// </summary>
    /// <param name="index"></param>
    /// <param name="on"></param>
    /// <param name="variableName"></param>
    /// <param name="valueName"></param>
    /// <returns></returns>
    public LazyFrame Melt(Selector index, Selector on, string variableName = "variable", string valueName = "value") 
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
    /// <seealso cref="DataFrame.Join(DataFrame, Expr[], Expr[], JoinType,string?,JoinValidation,JoinCoalesce,JoinMaintainOrder,bool,long?,ulong)"/>
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
    public LazyFrame Join(LazyFrame other,        
        Expr[] leftOn, 
        Expr[] rightOn, 
        JoinType how = JoinType.Inner,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
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
            how.ToNative(),
            suffix,
            validation.ToNative(),
            coalesce.ToNative(),
            maintainOrder.ToNative(),
            nullsEqual,
            sliceOffset,
            sliceLen
        ));
    }
    /// <summary>
    /// Join with another LazyFrame using column names.
    /// </summary>
    public LazyFrame Join(LazyFrame other,         
        string[] leftOn, 
        string[] rightOn, 
        JoinType how = JoinType.Inner,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        var lExprs = leftOn.Select(Polars.Col).ToArray();
        var rExprs = rightOn.Select(Polars.Col).ToArray();
        return Join(
            other, 
            lExprs, 
            rExprs, 
            how, 
            suffix, 
            validation, 
            coalesce, 
            maintainOrder, 
            nullsEqual, 
            sliceOffset, 
            sliceLen
        );
    }

    /// <summary>
    /// Join with another LazyFrame using a single column pair.
    /// </summary>
    public LazyFrame Join(LazyFrame other,
        string leftOn, 
        string rightOn, 
        JoinType how = JoinType.Inner,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        return Join(
            other, 
            [leftOn], 
            [rightOn], 
            how, 
            suffix, 
            validation, 
            coalesce, 
            maintainOrder, 
            nullsEqual, 
            sliceOffset, 
            sliceLen
        );
    }

    /// <summary>
    /// Perform an As-of join (also known as a time-series join).
    /// <para>
    /// This is similar to a left join except that we match on nearest key rather than equal keys.
    /// The join keys must be sorted.
    /// </para>
    /// </summary>
    /// <param name="other">The right LazyFrame to join with.</param>
    /// <param name="leftOn">Join key of the left LazyFrame. Must be sorted.</param>
    /// <param name="rightOn">Join key of the right LazyFrame. Must be sorted.</param>
    /// <param name="toleranceStr">
    /// Tolerance as a time duration string (e.g., "2h", "10s", "1d"). 
    /// Matches that are further away than this duration are discarded.
    /// </param>
    /// <param name="toleranceInt">
    /// Tolerance as a numeric integer (e.g., for integer-based timestamps or simple counters).
    /// </param>
    /// <param name="toleranceFloat">
    /// Tolerance as a floating point number.
    /// </param>
    /// <param name="strategy">
    /// The strategy to determine which value is "nearest" (Backward, Forward, or Nearest).
    /// Defaults to <see cref="AsofStrategy.Backward"/>.
    /// </param>
    /// <param name="leftBy">
    /// Columns to match exactly (equivalence join) before performing the as-of join. 
    /// Useful for joining separate time-series per group (e.g., by "Symbol").
    /// </param>
    /// <param name="rightBy">
    /// Columns to match exactly in the right DataFrame.
    /// </param>
    /// <param name="allowEq">
    /// If true, allow exact matches to be included in the result. 
    /// If false, a match must be strictly unequal (e.g. less than for Backward strategy) to the key.
    /// </param>
    /// <param name="checkSorted">
    /// Check if the join keys are sorted. 
    /// If false, the user must ensure keys are sorted; otherwise results are undefined (but execution is faster).
    /// </param>
    /// <param name="suffix">Suffix to append to columns with name conflicts. Defaults to "_right".</param>
    /// <param name="validation">Check if join keys are unique (mostly relevant for the 'by' columns).</param>
    /// <param name="coalesce">How to coalesce the join keys.</param>
    /// <param name="maintainOrder">How to maintain the order of the join.</param>
    /// <param name="nullsEqual">Consider nulls as equal.</param>
    /// <param name="sliceOffset">Slice the result starting at this offset (optimization).</param>
    /// <param name="sliceLen">Length of the slice to keep.</param>
    /// <example>
    /// <code>
    /// // Trades: Events happening at specific times
    /// var trades = DataFrame.FromColumns(new
    /// {
    ///     time = new[] { 10, 20, 30 },
    ///     stock = new[] { "A", "A", "A" }
    /// }).Lazy();
    /// 
    /// // Quotes: Price updates (irregular intervals)
    /// // 9->100, 15->101, 25->102, 40->103
    /// var quotes = DataFrame.FromColumns(new
    /// {
    ///     time = new[] { 9, 15, 25, 40 },
    ///     bid = new[] { 100, 101, 102, 103 }
    /// }).Lazy();
    /// 
    /// // Find the latest quote BEFORE or AT the trade time
    /// var asof = trades.JoinAsOf(
    ///     quotes, 
    ///     leftOn: Col("time"), 
    ///     rightOn: Col("time"),
    ///     strategy: AsofStrategy.Backward
    /// );
    /// 
    /// var df = asof.Collect();
    /// df.Show();
    /// /* Output:
    /// shape: (3, 3)
    /// ┌──────┬───────┬─────┐
    /// │ time ┆ stock ┆ bid │
    /// │ ---  ┆ ---   ┆ --- │
    /// │ i32  ┆ str   ┆ i32 │
    /// ╞══════╪═══════╪═════╡
    /// │ 10   ┆ A     ┆ 100 │ // Matches time 9
    /// │ 20   ┆ A     ┆ 101 │ // Matches time 15
    /// │ 30   ┆ A     ┆ 102 │ // Matches time 25
    /// └──────┴───────┴─────┘
    /// */
    /// </code>
    /// </example>
    internal LazyFrame JoinAsOf(
        LazyFrame other, 
        Expr leftOn, Expr rightOn, 
        string? toleranceStr = null,
        long? toleranceInt = null,
        double? toleranceFloat = null,
        AsofStrategy strategy = AsofStrategy.Backward,
        Expr[]? leftBy = null,
        Expr[]? rightBy = null,
        bool allowEq = true,
        bool checkSorted = true,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        var lfClone = CloneHandle();
        var otherClone = other.CloneHandle();
        
        var lOn = PolarsWrapper.CloneExpr(leftOn.Handle);
        var rOn = PolarsWrapper.CloneExpr(rightOn.Handle);
        
        var lBy = leftBy?.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        var rBy = rightBy?.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();

        return new LazyFrame(PolarsWrapper.JoinAsOf(
            lfClone, otherClone,
            [lOn], [rOn], // Wrap single Expr into array
            lBy, rBy,
            strategy.ToNative(),
            toleranceStr,
            toleranceInt,
            toleranceFloat,
            allowEq,
            checkSorted,
            suffix,
            validation.ToNative(),
            coalesce.ToNative(),
            maintainOrder.ToNative(),
            nullsEqual,
            sliceOffset,
            sliceLen
        ));
    }
    // 1. String Tolerance
    /// <inheritdoc cref="JoinAsOf(LazyFrame, Expr, Expr, string?, long?, double?, AsofStrategy, Expr[], Expr[], bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder, bool, long?, ulong)"/>
    /// <param name="tolerance">
    /// Tolerance as a time duration string (e.g., "2h", "10s", "1d"). 
    /// Matches that are further away than this duration are discarded.
    /// </param>
    public LazyFrame JoinAsOf(LazyFrame other, Expr leftOn, Expr rightOn, string tolerance, AsofStrategy strategy = AsofStrategy.Backward, Expr[]? leftBy = null, Expr[]? rightBy = null)
        => JoinAsOf(other, leftOn, rightOn, toleranceStr: tolerance, strategy: strategy, leftBy: leftBy, rightBy: rightBy);

    // 2. TimeSpan Tolerance
    /// <inheritdoc cref="JoinAsOf(LazyFrame, Expr, Expr, string?, long?, double?, AsofStrategy, Expr[], Expr[], bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder, bool, long?, ulong)"/>
    /// <param name="tolerance">
    /// Tolerance as a <see cref="TimeSpan"/>. 
    /// Matches that are further away than this duration are discarded.
    /// </param>
    public LazyFrame JoinAsOf(LazyFrame other, Expr leftOn, Expr rightOn, TimeSpan tolerance, AsofStrategy strategy = AsofStrategy.Backward, Expr[]? leftBy = null, Expr[]? rightBy = null)
        => JoinAsOf(other, leftOn, rightOn, toleranceStr: DurationFormatter.ToPolarsString(tolerance), strategy: strategy, leftBy: leftBy, rightBy: rightBy);

    // 3. Int Tolerance (e.g. integer timestamps)
    /// <inheritdoc cref="JoinAsOf(LazyFrame, Expr, Expr, string?, long?, double?, AsofStrategy, Expr[], Expr[], bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder, bool, long?, ulong)"/>
    /// <param name="tolerance">
    /// Tolerance as a numeric integer (e.g., for integer-based timestamps or simple counters).
    /// </param>
    public LazyFrame JoinAsOf(LazyFrame other, Expr leftOn, Expr rightOn, long tolerance, AsofStrategy strategy = AsofStrategy.Backward, Expr[]? leftBy = null, Expr[]? rightBy = null)
        => JoinAsOf(other, leftOn, rightOn, toleranceInt: tolerance, strategy: strategy, leftBy: leftBy, rightBy: rightBy);

    // 4. Double Tolerance (e.g. float keys)
    /// <inheritdoc cref="JoinAsOf(LazyFrame, Expr, Expr, string?, long?, double?, AsofStrategy, Expr[], Expr[], bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder, bool, long?, ulong)"/>
    /// <param name="tolerance">
    /// Tolerance as a floating point number.
    /// </param>
    public LazyFrame JoinAsOf(LazyFrame other, Expr leftOn, Expr rightOn, double tolerance, AsofStrategy strategy = AsofStrategy.Backward, Expr[]? leftBy = null, Expr[]? rightBy = null)
        => JoinAsOf(other, leftOn, rightOn, toleranceFloat: tolerance, strategy: strategy, leftBy: leftBy, rightBy: rightBy);
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
    public void SinkParquet(
        string path,
        ParquetCompression compression = ParquetCompression.Snappy,
        int compressionLevel = -1,
        bool statistics = false,
        int rowGroupSize = 0,
        int dataPageSize = 0,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false)
    {
        PolarsWrapper.SinkParquet(
            Handle,
            path,
            compression.ToNative(),
            compressionLevel,
            statistics,
            rowGroupSize,
            dataPageSize,
            maintainOrder,
            syncOnClose.ToNative(),
            mkdir
        );
    }
    /// <summary>
    /// Sink the LazyFrame to an IPC (Arrow) file.
    /// <br/>
    /// This allows writing datasets larger than memory by streaming the results to disk.
    /// </summary>
    /// <param name="path">The output file path.</param>
    /// <param name="compression">Compression method (None, LZ4, ZSTD). Defaults to None.</param>
    /// <param name="maintainOrder">Whether to maintain the order of the data. Defaults to true.</param>
    /// <param name="syncOnClose">File synchronization behavior on close. Defaults to None.</param>
    /// <param name="mkdir">Recursively create the directory if it does not exist. Defaults to false.</param>
    /// <param name="compatLevel">Arrow compatibility level. -1 means newest. Defaults to -1.</param>
    public void SinkIpc(
        string path,
        IpcCompression compression = IpcCompression.None,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        int compatLevel = -1)
    {
        PolarsWrapper.SinkIpc(
            Handle, 
            path, 
            compression.ToNative(), 
            compatLevel, 
            maintainOrder, 
            syncOnClose.ToNative(), 
            mkdir
        );
    }
    /// <summary>
    /// Sink the LazyFrame to a JSON file.
    /// </summary>
    /// <param name="path">Output file path.</param>
    /// <param name="format">JSON format (Json Array or JsonLines). Defaults to Json.</param>
    /// <param name="maintainOrder">Whether to maintain the order of the data. Defaults to true.</param>
    /// <param name="syncOnClose">File synchronization behavior on close. Defaults to None.</param>
    /// <param name="mkdir">Recursively create the directory if it does not exist. Defaults to false.</param>
    public void SinkJson(
        string path,
        JsonFormat format = JsonFormat.Json,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false)
    {
        PolarsWrapper.SinkJson(
            Handle,
            path,
            format.ToNative(),
            maintainOrder,
            syncOnClose.ToNative(),
            mkdir
        );
    }

    /// <summary>
    /// Alias for SinkJson with format=JsonLines.
    /// </summary>
    public void SinkNdJson(
        string path,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false)
    {
        SinkJson(path, JsonFormat.JsonLines, maintainOrder, syncOnClose, mkdir);
    }
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