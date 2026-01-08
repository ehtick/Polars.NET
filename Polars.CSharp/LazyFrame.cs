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
    /// <summary>
    /// Scan Arrow Stream As LazyFrame
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="data"></param>
    /// <param name="batchSize"></param>
    /// <returns></returns>
    public static LazyFrame ScanArrowStream<T>(IEnumerable<T> data, int batchSize = 100_000)
    {
        IEnumerable<RecordBatch> StreamGenerator() => data.ToArrowBatches(batchSize);

        using var probeEnumerator = StreamGenerator().GetEnumerator();
        
        if (!probeEnumerator.MoveNext()) 
        {
            return DataFrame.From(Enumerable.Empty<T>()).Lazy();
        }
        
        var schema = probeEnumerator.Current.Schema;

        var handle = ArrowStreamInterop.ScanStream(
            () => StreamGenerator().GetEnumerator(), 
            schema
        );
        
        return new LazyFrame(handle);
    }

    /// <summary>
    /// Scan Arrow Stream As LazyFrame with schema input
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="data"></param>
    /// <param name="schema"></param>
    /// <param name="batchSize"></param>
    /// <returns></returns>
    public static LazyFrame ScanArrowStream<T>(IEnumerable<T> data, Schema schema, int batchSize = 100_000)
    {
        var handle = ArrowStreamInterop.ScanStream(
            () => data.ToArrowBatches(batchSize).GetEnumerator(),
            schema
        );
        return new LazyFrame(handle);
    }

    /// <summary>
    /// Scan RecordBatch Stream
    /// If schema is provied, first batch won't be consumed for getting schema.
    /// </summary>
    public static LazyFrame ScanRecordBatches(IEnumerable<RecordBatch> stream, Schema schema = null!)
    {
        if (schema == null)
        {
            using var enumerator = stream.GetEnumerator();
            
            if (!enumerator.MoveNext())
                throw new InvalidOperationException("Cannot scan empty stream without schema. Please provide a schema explicitly.");
            
            schema = enumerator.Current.Schema;
        }

        var handle = ArrowStreamInterop.ScanStream(
            stream.GetEnumerator, 
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
    /// Lazy scan from a database using a factory.
    /// Recommended for scenarios where the query might be executed multiple times.
    /// </summary>
    /// <param name="readerFactory">A function that creates a NEW IDataReader instance each time.</param>
    /// <param name="batchSize">Define the size of the batch</param>
    public static LazyFrame ScanDatabase(Func<IDataReader> readerFactory, int batchSize = 50_000)
    {
        Schema schema;
        using (var probeReader = readerFactory())
        {
            schema = probeReader.GetArrowSchema();
        }

        IEnumerable<RecordBatch> ReplayableStream()
        {
            using var reader = readerFactory();
            
            foreach (var batch in reader.ToArrowBatches(batchSize))
            {
                yield return batch;
            }
            
        }

        return ScanRecordBatches(ReplayableStream(), schema);
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
    /// Get an explanation of the query plan.
    /// </summary>
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
    /// <param name="exprs"></param>
    /// <returns></returns>
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
    /// </summary>
    /// <param name="expr"></param>
    /// <returns></returns>
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
    /// <param name="exprs"></param>
    /// <returns></returns>
    public LazyFrame WithColumns(params Expr[] exprs)
    {
        var lfClone = CloneHandle();
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        return new LazyFrame(PolarsWrapper.LazyWithColumns(lfClone, handles));
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
    /// Sort the LazyFrame by multiple columns with specific sort orders.
    /// </summary>
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
    /// Unnest struct columns selected by a Selector.
    /// </summary>
    public LazyFrame Unnest(Selector selector)
    {
        var lfClone = CloneHandle();
        var sClone = selector.CloneHandle();
        var h = PolarsWrapper.LazyFrameUnnest(lfClone, sClone);
        return new LazyFrame(h);
    }

    /// <summary>
    /// Unnest specific struct columns by name.
    /// (Syntactic sugar for Unnest(Selector.Cols(...)))
    /// </summary>
    public LazyFrame Unnest(params string[] columns)
    {
        // 1. 在 C# 端创建 Selector 对象
        using var sel = Selector.Cols(columns);
        // 2. 传给底层
        return Unnest(sel);
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
    /// Concatenate multiple LazyFrames into one.
    /// </summary>
    /// <param name="how"></param>
    /// <param name="lfs"></param>
    /// <param name="rechunk"></param>
    /// <param name="parallel"></param>
    /// <returns></returns>
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
    /// Join with another LazyFrame on specified columns.
    /// </summary>
    /// <param name="other"></param>
    /// <param name="leftOn"></param>
    /// <param name="rightOn"></param>
    /// <param name="how"></param>
    /// <returns></returns>
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
    /// Start a GroupBy operation on specified keys.
    /// </summary>
    /// <param name="keys"></param>
    /// <returns></returns>
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
    /// Group by dynamic windows based on a time index.
    /// </summary>
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
    /// Generic streaming Sink interface: Streamingly convert LazyFrame calculation results to IDataReader 
    /// and hand it over to writerAction for processing.
    /// Users can utilize tools like SqlBulkCopy, NpgsqlBinaryImporter, etc. within writerAction.
    /// </summary>
    /// <param name="writerAction">Callback that receives IDataReader (executed in a separate thread)</param>
    /// <param name="bufferSize">Buffer size (number of Batches)</param>
    /// <param name="typeOverrides">Target Schema</param>
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