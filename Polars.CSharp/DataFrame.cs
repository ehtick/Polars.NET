using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using Apache.Arrow;
using System.Data;
using Polars.NET.Core.Data;
using System.Collections.Concurrent;
using System.Collections;
using System.Text;
using Apache.Arrow.Types;
using System.Reflection.Metadata;
using System.Reflection;

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
    /// Create a DataFrame directly from a <see cref="IDataReader"/>.
    /// <para>
    /// This method streams data from the reader into Arrow batches, allowing for memory-efficient 
    /// loading of large datasets from databases (e.g., SQL Server, PostgreSQL, SQLite).
    /// </para>
    /// <para>
    /// It automatically maps C# types to Polars types (e.g., <see cref="decimal"/> to Decimal128, <see cref="DateTime"/> to Timestamp).
    /// </para>
    /// </summary>
    /// <param name="reader">The open IDataReader instance.</param>
    /// <param name="batchSize">The number of rows to process per Arrow batch. Default is 50,000.</param>
    /// <returns>A new DataFrame.</returns>
    /// <example>
    /// <code>
    /// // Mocking a DataTable as a data source
    /// var table = new System.Data.DataTable();
    /// table.Columns.Add("Product", typeof(string));
    /// table.Columns.Add("Price", typeof(decimal)); // Correctly maps to Polars Decimal128
    /// 
    /// table.Rows.Add("Laptop", 1234.56m);
    /// table.Rows.Add("Mouse", 99.99m);
    /// 
    /// using IDataReader reader = table.CreateDataReader();
    /// 
    /// var df = DataFrame.ReadDatabase(reader);
    /// df.Show();
    /// /* Output:
    /// shape: (2, 2)
    /// ┌─────────┬─────────────────────────┐
    /// │ Product ┆ Price                   │
    /// │ ---     ┆ ---                     │
    /// │ str     ┆ decimal[38,18]          │
    /// ╞═════════╪═════════════════════════╡
    /// │ Laptop  ┆ 1234.560000000000000000 │
    /// │ Mouse   ┆ 99.990000000000000000   │
    /// └─────────┴─────────────────────────┘
    /// */
    /// </code>
    /// </example>
    public static DataFrame ReadDatabase(IDataReader reader, int batchSize = 50_000)
    {
        // Get Schema 
         var schema = reader.GetArrowSchema();

        // Get ArrowStream
        var batchEnumerable = reader.ToArrowBatches(batchSize);

        var handle = ArrowStreamInterop.ImportEager(batchEnumerable, schema);
        if (handle.IsInvalid)
        {
            var emptyBatch = new RecordBatch(schema, System.Array.Empty<IArrowArray>(), 0);
            return new DataFrame(ArrowFfiBridge.ImportDataFrame(emptyBatch));
        }
        return new DataFrame(handle);
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
    /// Return DataFrame Shape(Len,Width)
    /// </summary>
    public (long Len, long Width) Shape => (Len,Width);
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
    /// Select columns from the DataFrame and apply expressions to them.
    /// <para>
    /// This is the primary way to project data, rename columns, or compute new columns based on existing ones.
    /// The result will only contain the columns specified in the expression list.
    /// </para>
    /// </summary>
    /// <param name="exprs">A list of expressions defining the columns to select or compute.</param>
    /// <returns>A new DataFrame containing only the selected/computed columns.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     foo = new[] { 1, 2, 3 },
    ///     bar = new[] { 6, 7, 8 },
    ///     ham = new[] { "a", "b", "c" }
    /// });
    /// 
    /// // Select "foo" column and compute a new column "bar_x_2"
    /// var selected = df.Select(
    ///     Col("foo"),
    ///     (Col("bar") * 2).Alias("bar_x_2")
    /// );
    /// 
    /// selected.Show();
    /// /* Output:
    /// shape: (3, 2)
    /// ┌─────┬─────────┐
    /// │ foo ┆ bar_x_2 │
    /// │ --- ┆ ---     │
    /// │ i32 ┆ i32     │
    /// ╞═════╪═════════╡
    /// │ 1   ┆ 12      │
    /// │ 2   ┆ 14      │
    /// │ 3   ┆ 16      │
    /// └─────┴─────────┘
    /// */
    /// </code>
    /// </example>
    public DataFrame Select(params Expr[] exprs)
    {
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        return new DataFrame(PolarsWrapper.Select(Handle, handles));
    }
    /// <summary>
    /// Select columns by name (convenience overload).
    /// <para>
    /// This is a shortcut for creating <see cref="Polars.Col(string)"/> expressions for each column name.
    /// </para>
    /// </summary>
    /// <param name="columns">The names of the columns to select.</param>
    /// <returns>A new DataFrame containing only the selected columns.</returns>
    /// <remarks>
    /// For more advanced selections (renaming, calculations), use <see cref="Select(Expr[])"/>.
    /// </remarks>
    /// <seealso cref="Select(Expr[])"/>
    public DataFrame Select(params string[] columns)
    {
        var exprs = columns.Select(Polars.Col).ToArray();
        return Select(exprs);
    }
    /// <summary>
    /// Filter rows based on a boolean expression (predicate).
    /// <para>
    /// Retains only the rows where the expression evaluates to true.
    /// </para>
    /// </summary>
    /// <param name="expr">A boolean expression to filter by (e.g., Col("a") > 5).</param>
    /// <returns>A new DataFrame containing only the matching rows.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     foo = new[] { 1, 2, 3 },
    ///     bar = new[] { 6, 7, 8 },
    ///     ham = new[] { "a", "b", "c" }
    /// });
    /// 
    /// // Keep rows where "foo" is greater than 1
    /// var filtered = df.Filter(Col("foo") > 1);
    /// 
    /// filtered.Show();
    /// /* Output:
    /// shape: (2, 3)
    /// ┌─────┬─────┬─────┐
    /// │ foo ┆ bar ┆ ham │
    /// │ --- ┆ --- ┆ --- │
    /// │ i32 ┆ i32 ┆ str │
    /// ╞═════╪═════╪═════╡
    /// │ 2   ┆ 7   ┆ b   │
    /// │ 3   ┆ 8   ┆ c   │
    /// └─────┴─────┴─────┘
    /// */
    /// </code>
    /// </example>
    public DataFrame Filter(Expr expr)
    {
        var h = PolarsWrapper.CloneExpr(expr.Handle);

        return new DataFrame(PolarsWrapper.Filter(Handle, h));
    }
    /// <summary>
    /// Add new columns to the DataFrame or replace existing ones using expressions.
    /// <para>
    /// Unlike <see cref="Select(Expr[])"/>, this method keeps all original columns in the DataFrame 
    /// and appends the new ones (or replaces them if the names match).
    /// </para>
    /// </summary>
    /// <param name="exprs">Expressions defining the new columns to add.</param>
    /// <returns>A new DataFrame with the original columns plus the new/modified columns.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     foo = new[] { 1, 2, 3 },
    ///     bar = new[] { 6, 7, 8 },
    ///     ham = new[] { "a", "b", "c" }
    /// });
    /// 
    /// // Add a "sum" column (foo + bar) while keeping others
    /// var withCols = df.WithColumns(
    ///     (Col("foo") + Col("bar")).Alias("sum")
    /// );
    /// 
    /// withCols.Show();
    /// /* Output:
    /// shape: (3, 4)
    /// ┌─────┬─────┬─────┬─────┐
    /// │ foo ┆ bar ┆ ham ┆ sum │
    /// │ --- ┆ --- ┆ --- ┆ --- │
    /// │ i32 ┆ i32 ┆ str ┆ i32 │
    /// ╞═════╪═════╪═════╪═════╡
    /// │ 1   ┆ 6   ┆ a   ┆ 7   │
    /// │ 2   ┆ 7   ┆ b   ┆ 9   │
    /// │ 3   ┆ 8   ┆ c   ┆ 11  │
    /// └─────┴─────┴─────┴─────┘
    /// */
    /// </code>
    /// </example>
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
    /// Sort the DataFrame by multiple columns with specific sort orders for each column.
    /// <para>
    /// This allows for complex sorting, such as sorting by Category (Ascending) and then by Price (Descending).
    /// </para>
    /// </summary>
    /// <param name="columns">Names of the columns to sort by.</param>
    /// <param name="descending">Array of booleans indicating sort order for each column (false=Ascending, true=Descending).</param>
    /// <param name="nullsLast">Array of booleans indicating whether nulls should be placed at the end for each column.</param>
    /// <param name="maintainOrder">Whether to maintain the relative order of rows with equal keys (stable sort). Expensive.</param>
    /// <returns>A new sorted DataFrame.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     group = new[] { "A", "A", "B", "B", "A" },
    ///     val = new[] { 10, 5, 20, 15, 8 }
    /// });
    /// 
    /// // Sort by "group" (Ascending) then by "val" (Descending)
    /// var sorted = df.Sort(
    ///     columns: new[] { "group", "val" }, 
    ///     descending: new[] { false, true }, // false=Asc, true=Desc
    ///     nullsLast: new[] { false, false }
    /// );
    /// 
    /// sorted.Show();
    /// /* Output:
    /// shape: (5, 2)
    /// ┌───────┬─────┐
    /// │ group ┆ val │
    /// │ ---   ┆ --- │
    /// │ str   ┆ i32 │
    /// ╞═══════╪═════╡
    /// │ A     ┆ 10  │
    /// │ A     ┆ 8   │
    /// │ A     ┆ 5   │
    /// │ B     ┆ 20  │
    /// │ B     ┆ 15  │
    /// └───────┴─────┘
    /// */
    /// </code>
    /// </example>
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
    /// Explode list/array columns into multiple rows.
    /// <para>
    /// This un-nests list columns. If multiple columns are provided, they are exploded in parallel.
    /// Note: The list columns being exploded must have the same length for each row.
    /// </para>
    /// </summary>
    /// <param name="exprs">Expressions selecting the columns to explode.</param>
    /// <returns>A new DataFrame where list elements are expanded into rows.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     id = new[] { 1, 2 },
    ///     tags = new[] { new[] { "apple", "orange" }, new[] { "banana" } },
    ///     scores = new[] { new[] { 10, 20 }, new[] { 30 } }
    /// });
    /// 
    /// // Explode "tags" and "scores" columns simultaneously
    /// var exploded = df.Explode(Col("tags"), Col("scores"));
    /// 
    /// exploded.Show();
    /// /* Output:
    /// shape: (3, 3)
    /// ┌─────┬────────┬────────┐
    /// │ id  ┆ tags   ┆ scores │
    /// │ --- ┆ ---    ┆ ---    │
    /// │ i32 ┆ str    ┆ i32    │
    /// ╞═════╪════════╪════════╡
    /// │ 1   ┆ apple  ┆ 10     │
    /// │ 1   ┆ orange ┆ 20     │
    /// │ 2   ┆ banana ┆ 30     │
    /// └─────┴────────┴────────┘
    /// */
    /// </code>
    /// </example>
    public DataFrame Explode(params Expr[] exprs)
    {
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        return new DataFrame(PolarsWrapper.Explode(Handle, handles));
    }
    /// <summary>
    /// Decompose a struct column into multiple columns.
    /// <para>
    /// This is useful for flattening nested JSON-like data.
    /// </para>
    /// </summary>
    /// <param name="columns">The names of the struct columns to unnest.</param>
    /// <param name="separator">
    /// Optional separator to append to the struct field names (e.g., "struct_col.field"). 
    /// If null, the field names replace the struct column name directly.
    /// </param>
    /// <returns>A new DataFrame with the struct columns expanded.</returns>
    /// <example>
    /// <code>
    /// var data = new[] { new { User = new { Name = "Alice", Age = 30 } } };
    /// var df = DataFrame.From(data);
    /// 
    /// // Unnest with a separator to avoid name collisions
    /// // Result columns: "User_Name", "User_Age"
    /// var unnested = df.Unnest(new[] { "User" }, separator: "_");
    /// unnested.Show();
    /// /* Output:
    /// shape: (1, 2)
    /// ┌───────────┬──────────┐
    /// │ User_Name ┆ User_Age │
    /// │ ---       ┆ ---      │
    /// │ str       ┆ i32      │
    /// ╞═══════════╪══════════╡
    /// │ Alice     ┆ 30       │
    /// └───────────┴──────────┘
    /// */
    /// </code>
    /// </example>
    public DataFrame Unnest(string[] columns,string? separator=null)
    {
        var newHandle = PolarsWrapper.Unnest(Handle, columns,separator);
        return new DataFrame(newHandle);
    }
    /// <summary>
    /// Decompose a struct column into multiple columns (one for each field in the struct).
    /// <para>
    /// This is useful for flattening nested JSON-like data or composite types.
    /// The original struct column is replaced by its fields.
    /// </para>
    /// </summary>
    /// <param name="columns">The names of the struct columns to unnest.</param>
    /// <returns>A new DataFrame with the struct columns expanded.</returns>
    /// <example>
    /// <code>
    /// // Create a DataFrame with a nested structure (simulating JSON)
    /// var nestedData = new[] 
    /// {
    ///     new { Id = 1, User = new { Name = "Alice", City = "NY" } },
    ///     new { Id = 2, User = new { Name = "Bob",   City = "LA" } }
    /// };
    /// 
    /// var df = DataFrame.From(nestedData);
    /// 
    /// // Unnest the "User" column into "Name" and "City"
    /// var unnested = df.Unnest("User");
    /// 
    /// unnested.Show();
    /// /* Output:
    /// shape: (2, 3)
    /// ┌─────┬───────┬──────┐
    /// │ Id  ┆ Name  ┆ City │
    /// │ --- ┆ ---   ┆ ---  │
    /// │ i32 ┆ str   ┆ str  │
    /// ╞═════╪═══════╪══════╡
    /// │ 1   ┆ Alice ┆ NY   │
    /// │ 2   ┆ Bob   ┆ LA   │
    /// └─────┴───────┴──────┘
    /// */
    /// </code>
    /// </example>
    public DataFrame Unnest(params string[] columns) => Unnest(columns, separator: null);
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
    /// <summary>
    /// Slice the DataFrame along the rows.
    /// </summary>
    /// <param name="offset">Start index. Negative values work as expected (counting from the end).</param>
    /// <param name="length">Length of the slice.</param>
    /// <returns>A new sliced DataFrame.</returns>
    public DataFrame Slice(long offset, ulong length)
        => new(PolarsWrapper.Slice(Handle, offset, length));
    /// <summary>
    /// Slice the DataFrame along the rows (Convenience overload for int).
    /// </summary>
    public DataFrame Slice(int offset, int length)
    {
        if (length < 0) throw new ArgumentOutOfRangeException(nameof(length), "Length must be non-negative.");
        return Slice((long)offset, (ulong)length);
    }

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
    /// Join with another DataFrame using expressions to define the keys.
    /// <para>
    /// This is the most flexible join method, allowing for joining on calculations or different column names 
    /// without renaming them first.
    /// </para>
    /// </summary>
    /// <param name="other">The right DataFrame to join with.</param>
    /// <param name="leftOn">Expressions for the left keys.</param>
    /// <param name="rightOn">Expressions for the right keys.</param>
    /// <param name="how">Type of join (Inner, Left, Outer, Cross, etc.). Default is Inner.</param>
    /// <returns>A new DataFrame resulting from the join.</returns>
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
    /// <param name="other">The right DataFrame to join with.</param>
    /// <param name="leftOn">Column names in the left DataFrame to join on.</param>
    /// <param name="rightOn">Column names in the right DataFrame to join on.</param>
    /// <param name="how">Type of join (Inner, Left, Outer, Cross, etc.). Default is Inner.</param>
    /// <returns>A new DataFrame resulting from the join.</returns>
    /// <example>
    /// <code>
    /// var dfCustomers = DataFrame.FromColumns(new
    /// {
    ///     id = new[] { 1, 2, 3 },
    ///     name = new[] { "Alice", "Bob", "Charlie" }
    /// });
    /// 
    /// var dfOrders = DataFrame.FromColumns(new
    /// {
    ///     id = new[] { 1, 2, 4 },
    ///     amount = new[] { 100, 200, 500 }
    /// });
    /// 
    /// // Perform an Inner Join on the "id" column
    /// var joined = dfCustomers.Join(
    ///     dfOrders, 
    ///     leftOn: new[] { "id" }, 
    ///     rightOn: new[] { "id" }, 
    ///     how: JoinType.Inner
    /// );
    /// 
    /// joined.Show();
    /// /* Output:
    /// shape: (2, 3)
    /// ┌─────┬───────┬────────┐
    /// │ id  ┆ name  ┆ amount │
    /// │ --- ┆ ---   ┆ ---    │
    /// │ i32 ┆ str   ┆ i32    │
    /// ╞═════╪═══════╪════════╡
    /// │ 1   ┆ Alice ┆ 100    │
    /// │ 2   ┆ Bob   ┆ 200    │
    /// └─────┴───────┴────────┘
    /// */
    /// </code>
    /// </example>
    public DataFrame Join(DataFrame other, string[] leftOn, string[] rightOn, JoinType how = JoinType.Inner)
    {
        var lExprs = leftOn.Select(c => Polars.Col(c)).ToArray();
        var rExprs = rightOn.Select(c => Polars.Col(c)).ToArray();

        return Join(other, lExprs, rExprs, how);
    }

    /// <summary>
    /// Join with another DataFrame using a single column pair (Convenience overload).
    /// </summary>
    /// <param name="other">The right DataFrame to join with.</param>
    /// <param name="leftOn">The column name in the left DataFrame.</param>
    /// <param name="rightOn">The column name in the right DataFrame.</param>
    /// <param name="how">Type of join. Default is Inner.</param>
    /// <seealso cref="Join(DataFrame, string[], string[], JoinType)"/>
    public DataFrame Join(DataFrame other, string leftOn, string rightOn, JoinType how = JoinType.Inner)
        => Join(other, [leftOn], [rightOn], how);
    /// <summary>
    /// Perform an As-Of Join (time-series join).
    /// <para>
    /// This is similar to a left join, but instead of looking for exact matches, it matches the nearest key 
    /// according to the specified <paramref name="strategy"/>.
    /// This is ideal for joining trades with quotes, or synchronizing time-series data with different timestamps.
    /// </para>
    /// </summary>
    /// <param name="other">The right DataFrame to join with.</param>
    /// <param name="leftOn">The expression for the key column in the left DataFrame (must be sorted).</param>
    /// <param name="rightOn">The expression for the key column in the right DataFrame (must be sorted).</param>
    /// <param name="tolerance">Optional tolerance (e.g., "10s", "1d"). Matches beyond this distance are null.</param>
    /// <param name="strategy">Match strategy: "backward" (default, look for previous value), "forward", or "nearest".</param>
    /// <param name="leftBy">Optional columns to group by on the left side (exact match required for these).</param>
    /// <param name="rightBy">Optional columns to group by on the right side (exact match required for these).</param>
    /// <returns>A new DataFrame with the joined data.</returns>
    /// <example>
    /// <code>
    /// // Trades: Events happening at specific times
    /// var trades = DataFrame.FromColumns(new
    /// {
    ///     time = new[] { 10, 20, 30 },
    ///     stock = new[] { "A", "A", "A" }
    /// });
    /// 
    /// // Quotes: Price updates (irregular intervals)
    /// // 9->100, 15->101, 25->102, 40->103
    /// var quotes = DataFrame.FromColumns(new
    /// {
    ///     time = new[] { 9, 15, 25, 40 },
    ///     bid = new[] { 100, 101, 102, 103 }
    /// });
    /// 
    /// // Find the latest quote BEFORE or AT the trade time (strategy="backward")
    /// var asof = trades.JoinAsOf(
    ///     quotes, 
    ///     leftOn: Col("time"), 
    ///     rightOn: Col("time"),
    ///     strategy: "backward"
    /// );
    /// 
    /// asof.Show();
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
    /// Perform an As-Of Join with tolerance as a TimeSpan.
    /// <para>
    /// Convenience overload that formats the <see cref="TimeSpan"/> into a Polars duration string.
    /// </para>
    /// </summary>
    /// <seealso cref="JoinAsOf(DataFrame, Expr, Expr, string?, string, Expr[], Expr[])"/>
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
    /// Concatenate multiple DataFrames either vertically (union) or horizontally.
    /// </summary>
    /// <param name="dfs">A collection of DataFrames to concatenate.</param>
    /// <param name="how">The concatenation strategy (Vertical, Horizontal, or Diagonal). Default is Vertical.</param>
    /// <returns>A new combined DataFrame.</returns>
    /// <example>
    /// <code>
    /// var df1 = DataFrame.FromColumns(new
    /// {
    ///     a = new[] { 1, 2 },
    ///     b = new[] { 10, 20 }
    /// });
    /// 
    /// var df2 = DataFrame.FromColumns(new
    /// {
    ///     a = new[] { 3, 4 },
    ///     b = new[] { 30, 40 }
    /// });
    /// 
    /// // Vertical Concat (Union rows)
    /// var catVertical = DataFrame.Concat(new[] { df1, df2 });
    /// catVertical.Show();
    /// /* Output:
    /// shape: (4, 2)
    /// ┌─────┬─────┐
    /// │ a   ┆ b   │
    /// │ --- ┆ --- │
    /// │ i32 ┆ i32 │
    /// ╞═════╪═════╡
    /// │ 1   ┆ 10  │
    /// │ 2   ┆ 20  │
    /// │ 3   ┆ 30  │
    /// │ 4   ┆ 40  │
    /// └─────┴─────┘
    /// */
    /// 
    /// // Horizontal Concat (Append columns)
    /// var df3 = DataFrame.FromColumns(new { c = new[] { "x", "y" } });
    /// var catHorizontal = DataFrame.Concat(new[] { df1, df3 }, ConcatType.Horizontal);
    /// catHorizontal.Show();
    /// /* Output:
    /// shape: (2, 3)
    /// ┌─────┬─────┬─────┐
    /// │ a   ┆ b   ┆ c   │
    /// │ --- ┆ --- ┆ --- │
    /// │ i32 ┆ i32 ┆ str │
    /// ╞═════╪═════╪═════╡
    /// │ 1   ┆ 10  ┆ x   │
    /// │ 2   ┆ 20  ┆ y   │
    /// └─────┴─────┴─────┘
    /// */
    /// </code>
    /// </example>
    public static DataFrame Concat(IEnumerable<DataFrame> dfs, ConcatType how = ConcatType.Vertical)
    {
        var handles = dfs.Select(d => PolarsWrapper.CloneDataFrame(d.Handle)).ToArray();
        
        return new DataFrame(PolarsWrapper.Concat(handles, how.ToNative()));
    }

    // ==========================================
    // GroupBy
    // ==========================================
    /// <summary>
    /// Start a GroupBy operation to perform aggregations on groups of data.
    /// <para>
    /// This returns a <see cref="GroupByBuilder"/> which allows you to specify the aggregation functions 
    /// (like Sum, Min, Max, Count) using the <c>.Agg()</c> method.
    /// </para>
    /// </summary>
    /// <param name="by">Expressions defining the columns to group by.</param>
    /// <returns>A builder object to define aggregations.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     group = new[] { "A", "A", "B", "B", "B" },
    ///     val1 = new[] { 1, 2, 3, 4, 5 },
    ///     val2 = new[] { 10, 20, 10, 20, 30 }
    /// });
    /// 
    /// // Group by "group" and calculate:
    /// // 1. Sum of val1
    /// // 2. Max of val2 (aliased to "max_val2")
    /// // 3. Count of elements in the group
    /// var grouped = df.GroupBy(Col("group")).Agg(
    ///     Col("val1").Sum(),
    ///     Col("val2").Max().Alias("max_val2"),
    ///     Col("val1").Count().Alias("count")
    /// );
    /// 
    /// grouped.Sort("group").Show();
    /// /* Output:
    /// shape: (2, 4)
    /// ┌───────┬──────┬──────────┬───────┐
    /// │ group ┆ val1 ┆ max_val2 ┆ count │
    /// │ ---   ┆ ---  ┆ ---      ┆ ---   │
    /// │ str   ┆ i32  ┆ i32      ┆ u32   │
    /// ╞═══════╪══════╪══════════╪═══════╡
    /// │ A     ┆ 3    ┆ 20       ┆ 2     │
    /// │ B     ┆ 12   ┆ 30       ┆ 3     │
    /// └───────┴──────┴──────────┴───────┘
    /// */
    /// </code>
    /// </example>
    public GroupByBuilder GroupBy(params Expr[] by) => new (this, by);
    /// <summary>
    /// Start a GroupBy operation using column names (convenience overload).
    /// </summary>
    /// <param name="columns">Names of the columns to group by.</param>
    /// <returns>A builder object to define aggregations.</returns>
    /// <remarks>
    /// For more complex grouping logic (expressions), use <see cref="GroupBy(Expr[])"/>.
    /// </remarks>
    /// <seealso cref="GroupBy(Expr[])"/>
    public GroupByBuilder GroupBy(params string[] columns)
    {
        var exprs = columns.Select(Polars.Col).ToArray();
        return GroupBy(exprs);
    }
    /// <summary>
    /// Group based on a time index using dynamic windows (Rolling/Resampling).
    /// <para>
    /// This is essential for time-series analysis, allowing you to downsample data (e.g., 1-minute data to 1-hour bars).
    /// </para>
    /// </summary>
    /// <param name="indexColumn">The column containing time/date data (must be sorted).</param>
    /// <param name="every">The interval at which to start a new window (e.g., "1h", "1d"). Also known as the step size.</param>
    /// <param name="period">The duration of each window. If null, defaults to <paramref name="every"/>.</param>
    /// <param name="offset">Offset to shift the window start times.</param>
    /// <param name="by">Optional extra columns to group by (e.g., group by "stock_symbol" AND time window).</param>
    /// <param name="label">Which label to use for the window (Left boundary, Right boundary, or Datapoint).</param>
    /// <param name="includeBoundaries">Whether to include the window boundaries in the output.</param>
    /// <param name="closedWindow">Which side of the window interval is closed (inclusive).</param>
    /// <param name="startBy">Strategy to determine the start of the first window.</param>
    /// <returns>A <see cref="DynamicGroupBy"/> object to define aggregations.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     time = new[] 
    ///     { 
    ///         new DateTime(2024, 1, 1, 10, 0, 0),
    ///         new DateTime(2024, 1, 1, 10, 30, 0), // Inside 10:00-11:00 window
    ///         new DateTime(2024, 1, 1, 11, 0, 0), // Start of next window
    ///         new DateTime(2024, 1, 1, 11, 15, 0),
    ///         new DateTime(2024, 1, 2, 09, 0, 0)
    ///     },
    ///     val = new[] { 1, 2, 3, 4, 5 }
    /// });
    /// 
    /// // Group into 1-hour windows based on "time"
    /// var dynamicGrouped = df.GroupByDynamic(
    ///     indexColumn: "time", 
    ///     every: TimeSpan.FromHours(1)
    /// ).Agg(
    ///     Col("val").Sum().Alias("total_val"),
    ///     Col("val").Count().Alias("count")
    /// );
    /// 
    /// dynamicGrouped.Show();
    /// /* Output:
    /// shape: (3, 3)
    /// ┌─────────────────────┬───────────┬───────┐
    /// │ time                ┆ total_val ┆ count │
    /// │ ---                 ┆ ---       ┆ ---   │
    /// │ datetime[μs]        ┆ i32       ┆ u32   │
    /// ╞═════════════════════╪═══════════╪═══════╡
    /// │ 2024-01-01 10:00:00 ┆ 3         ┆ 2     │ // 10:00 + 10:30
    /// │ 2024-01-01 11:00:00 ┆ 7         ┆ 2     │ // 11:00 + 11:15
    /// │ 2024-01-02 09:00:00 ┆ 5         ┆ 1     │
    /// └─────────────────────┴───────────┴───────┘
    /// */
    /// </code>
    /// </example>
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
    /// <para>
    /// This creates a new column for each unique value in the <paramref name="columns"/> argument.
    /// The values in the new columns come from the <paramref name="values"/> column.
    /// </para>
    /// </summary>
    /// <param name="index">Column names to use as the index (the rows).</param>
    /// <param name="columns">Column names to use for the new column headers.</param>
    /// <param name="values">Column names to use for the values in the cells.</param>
    /// <param name="agg">Aggregation function to use if multiple values exist for an index/column pair. Default is First.</param>
    /// <returns>A wide-format DataFrame.</returns>
    /// <example>
    /// <code>
    /// var dfLong = DataFrame.FromColumns(new
    /// {
    ///     date = new[] { "2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02" },
    ///     country = new[] { "US", "CN", "US", "CN" },
    ///     sales = new[] { 100, 200, 110, 220 }
    /// });
    /// 
    /// // Pivot: rows=date, cols=country, values=sales
    /// var pivoted = dfLong.Pivot(
    ///     index: new[] { "date" }, 
    ///     columns: new[] { "country" }, 
    ///     values: new[] { "sales" }
    /// );
    /// 
    /// pivoted.Show();
    /// /* Output:
    /// shape: (2, 3)
    /// ┌────────────┬─────┬─────┐
    /// │ date       ┆ US  ┆ CN  │
    /// │ ---        ┆ --- ┆ --- │
    /// │ str        ┆ i32 ┆ i32 │
    /// ╞════════════╪═════╪═════╡
    /// │ 2024-01-01 ┆ 100 ┆ 200 │
    /// │ 2024-01-02 ┆ 110 ┆ 220 │
    /// └────────────┴─────┴─────┘
    /// */
    /// </code>
    /// </example>
    public DataFrame Pivot(string[] index, string[] columns, string[] values, PivotAgg agg = PivotAgg.First)
        => new(PolarsWrapper.Pivot(Handle, index, columns, values, agg.ToNative()));
    /// <summary>
    /// Unpivot (Melt) the DataFrame from wide to long format.
    /// <para>
    /// This is the reverse of <see cref="Pivot"/>. It collapses multiple columns into key-value pairs.
    /// </para>
    /// </summary>
    /// <param name="index">Column names to keep as identifiers (id_vars).</param>
    /// <param name="on">Column names to unpivot/melt (value_vars).</param>
    /// <param name="variableName">Name for the new variable column (default "variable").</param>
    /// <param name="valueName">Name for the new value column (default "value").</param>
    /// <returns>A long-format DataFrame.</returns>
    /// <example>
    /// <code>
    /// // Using the 'pivoted' DataFrame from the Pivot example
    /// // shape: (2, 3) [date, US, CN]
    /// 
    /// // Unpivot back to long format
    /// var melted = pivoted.Unpivot(
    ///     index: new[] { "date" },
    ///     on: new[] { "CN", "US" },
    ///     variableName: "country",
    ///     valueName: "sales"
    /// );
    /// 
    /// melted.Sort(new[] { "date", "country" }).Show();
    /// /* Output:
    /// shape: (4, 3)
    /// ┌────────────┬─────────┬───────┐
    /// │ date       ┆ country ┆ sales │
    /// │ ---        ┆ ---     ┆ ---   │
    /// │ str        ┆ str     ┆ i32   │
    /// ╞════════════╪═════════╪═══════╡
    /// │ 2024-01-01 ┆ CN      ┆ 200   │
    /// │ 2024-01-01 ┆ US      ┆ 100   │
    /// │ 2024-01-02 ┆ CN      ┆ 220   │
    /// │ 2024-01-02 ┆ US      ┆ 110   │
    /// └────────────┴─────────┴───────┘
    /// */
    /// </code>
    /// </example>
    public DataFrame Unpivot(string[] index, string[] on, string variableName = "variable", string valueName = "value")
        => new(PolarsWrapper.Unpivot(Handle, index, on, variableName, valueName));
    /// <summary>
    /// Alias for <see cref="Unpivot"/>. Melts the DataFrame from wide to long format.
    /// </summary>
    /// <seealso cref="Unpivot(string[], string[], string, string)"/>
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
            ("25%",        c => Polars.Col(c).Quantile(0.25, QuantileMethod.Nearest).Cast(DataType.Float64)),
            ("50%",        c => Polars.Col(c).Median().Cast(DataType.Float64)),
            ("75%",        c => Polars.Col(c).Quantile(0.75, QuantileMethod.Nearest).Cast(DataType.Float64)),
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
    private static bool TryCreateSeriesFast(string name, object values, out Series? series)
    {
        series = null;
        if (values == null) return false;

        switch (values)
        {
            // --- Signed Integers ---
            case sbyte[] v:    series = new Series(name, v); return true;
            case sbyte?[] v:   series = new Series(name, v); return true;
            case short[] v:    series = new Series(name, v); return true;
            case short?[] v:   series = new Series(name, v); return true;
            case int[] v:      series = new Series(name, v); return true;
            case int?[] v:     series = new Series(name, v); return true;
            case long[] v:     series = new Series(name, v); return true;
            case long?[] v:    series = new Series(name, v); return true;
            case Int128[] v:   series = new Series(name, v); return true;
            case Int128?[] v:  series = new Series(name, v); return true;

            // --- Unsigned Integers (Zero-Copy Cast inside Series) ---
            case byte[] v:     series = new Series(name, v); return true;
            case byte?[] v:    series = new Series(name, v); return true;
            case ushort[] v:   series = new Series(name, v); return true;
            case ushort?[] v:  series = new Series(name, v); return true;
            case uint[] v:     series = new Series(name, v); return true;
            case uint?[] v:    series = new Series(name, v); return true;
            case ulong[] v:    series = new Series(name, v); return true;
            case ulong?[] v:   series = new Series(name, v); return true;
            case UInt128[] v:  series = new Series(name, v); return true;
            case UInt128?[] v: series = new Series(name, v); return true;

            // --- Floating Point ---
            case float[] v:    series = new Series(name, v); return true;
            case float?[] v:   series = new Series(name, v); return true;
            case double[] v:   series = new Series(name, v); return true;
            case double?[] v:  series = new Series(name, v); return true;
            
            // --- Boolean (Bit Packing) ---
            case bool[] v:     series = new Series(name, v); return true;
            case bool?[] v:    series = new Series(name, v); return true;

            // --- String ---
            case string[] v:   series = new Series(name, v); return true;
            // case string?[] v:  series = new Series(name, v); return true;
                
            // --- Default ---
            default:
                return false; 
        }
    }
    /// <summary>
    /// Create a DataFrame from a collection of strongly-typed objects (POCOs).
    /// <para>
    /// This method uses reflection to inspect the properties of the class <typeparamref name="T"/> 
    /// and maps them to DataFrame columns.
    /// </para>
    /// </summary>
    /// <typeparam name="T">The class type of the records.</typeparam>
    /// <param name="data">The collection of objects to load.</param>
    /// <returns>A new DataFrame.</returns>
    /// <example>
    /// <code>
    /// public class Student
    /// {
    ///     public string Name { get; set; }
    ///     public int Age { get; set; }
    ///     public double GPA { get; set; }
    /// }
    /// 
    /// var students = new List&lt;Student&gt;
    /// {
    ///     new Student { Name = "Alice", Age = 20, GPA = 3.5 },
    ///     new Student { Name = "Bob", Age = 22, GPA = 3.8 },
    ///     new Student { Name = "Charlie", Age = 19, GPA = 3.2 }
    /// };
    /// 
    /// var df = DataFrame.From(students);
    /// df.Show();
    /// /* Output:
    /// shape: (3, 3)
    /// ┌─────────┬─────┬─────┐
    /// │ Name    ┆ Age ┆ GPA │
    /// │ ---     ┆ --- ┆ --- │
    /// │ str     ┆ i32 ┆ f64 │
    /// ╞═════════╪═════╪═════╡
    /// │ Alice   ┆ 20  ┆ 3.5 │
    /// │ Bob     ┆ 22  ┆ 3.8 │
    /// │ Charlie ┆ 19  ┆ 3.2 │
    /// └─────────┴─────┴─────┘
    /// */
    /// </code>
    /// </example>
    public static DataFrame From<T>(IEnumerable<T> data)
    {
        if (data == null) return new DataFrame();

        Type type = typeof(T);

        // =========================================================
        // Primitive Types: int, double, string...
        // =========================================================
        if (IsSimpleType(type))
        {
            T[] array = data as T[] ?? data.ToArray();
            
            if (TryCreateSeriesFast("value", array, out var s))
            {
                return new DataFrame(s!);
            }
            
            // Fallback to Arrow
            var arrowArray = ArrowConverter.BuildSingleColumn(array);
            if (arrowArray != null)
            {
                using var sArrow = Series.FromArrow("value", arrowArray);
                return new DataFrame(sArrow);
            }
            
            throw new NotSupportedException($"Type {type.Name} is not supported for single-column DataFrame.");
        }

        // =========================================================
        // Complex Type: Pivot (Row -> Column)
        // =========================================================
        var props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        if (props.Length == 0) return new DataFrame();

        int capacity = data is ICollection<T> c ? c.Count : 16;

        // Column Buffers
        var buffers = new IColumnBuffer[props.Length];
        for (int i = 0; i < props.Length; i++)
        {
            buffers[i] = ColumnBufferFactory.Create(props[i].PropertyType, capacity);
        }

        foreach (var item in data)
        {
            for (int i = 0; i < props.Length; i++)
            {
                var val = props[i].GetValue(item);
                buffers[i].Add(val!);
            }
        }

        // =========================================================
        // Build Series then combine to DataFrame
        // =========================================================
        var seriesList = new Series[props.Length];
        for (int i = 0; i < props.Length; i++)
        {
            string name = props[i].Name;
            seriesList[i] = buffers[i].ToSeries(name);
        }

        return new DataFrame(seriesList);
    }

    private static bool IsSimpleType(Type type)
    {
        return type.IsPrimitive || 
               type == typeof(string) || 
               type == typeof(decimal) || 
               type == typeof(DateTime) || 
               type == typeof(TimeSpan) ||
               (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>) && IsSimpleType(Nullable.GetUnderlyingType(type)!));
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
        ArgumentNullException.ThrowIfNull(columns);

        var properties = columns.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);
        
        var cols = new (string, object)[properties.Length];

        for (int i = 0; i < properties.Length; i++)
        {
            var p = properties[i];
            var val = p.GetValue(columns) ?? throw new ArgumentNullException($"Property '{p.Name}' cannot be null.");
            cols[i] = (p.Name, val);
        }

        return FromColumns(cols);
    }
    /// <summary>
    /// [AOT Safe] Create DataFrame from explicitly named columns.
    /// No reflection used. Best performance.
    /// </summary>
    /// <exception cref="ArgumentNullException"></exception>
    /// <exception cref="NotSupportedException"></exception>
    public static DataFrame FromColumns(params (string Name, object Data)[] columns)
    {
        if (columns == null || columns.Length == 0)
            return new DataFrame(); // Return empty DF

        var seriesList = new List<Series>(columns.Length);

        foreach (var (name, val) in columns.AsSpan())
        {
            if (val == null)
                throw new ArgumentNullException($"Column '{name}' data cannot be null.");

            // SIMD Highway
            if (TryCreateSeriesFast(name, val, out var fastSeries))
            {
                seriesList.Add(fastSeries!);
            }
            else
            {
                // Fallback to Arrow
                var arrowArray = ArrowConverter.BuildSingleColumn(val);

                if (arrowArray != null)
                {
                    seriesList.Add(Series.FromArrow(name, arrowArray));
                }
                else
                {
                    throw new NotSupportedException(
                        $"Column '{name}' has unsupported data type: {val.GetType().Name}. " +
                        "Only primitives (int, double, bool...), strings, DateTimes, and IEnumerables supported by Arrow are allowed.");
                }
            }
        }

        return new DataFrame(seriesList.ToArray());
    }
    // =========================================================
    // Internal Column Buffers
    // =========================================================

    private interface IColumnBuffer
    {
        void Add(object? val);
        Series ToSeries(string name);
    }

    /// <summary>
    /// Strong Type Column Buffer 
    /// </summary>
    /// <typeparam name="TCol">Column Type(Example: int, string, DateTime?)</typeparam>
    private sealed class ColumnBuffer<TCol> : IColumnBuffer
    {
        private readonly List<TCol?> _data; 

        public ColumnBuffer(int capacity)
        {
            _data = new List<TCol?>(capacity);
        }

        public void Add(object? val)
        {
            _data.Add((TCol?)val);
        }

        public Series ToSeries(string name)
        {
            TCol?[] arr = _data.ToArray(); 
            
            // SIMD Highway (Int32[], Double[]...)
            if (TryCreateSeriesFast(name, arr, out var s))
                return s!;
            
            // Fallback to Arrow (List<int>, Decimal...)
            var arrowArr = ArrowConverter.BuildSingleColumn(arr);
            if (arrowArr != null) 
                return Series.FromArrow(name, arrowArr);
            
            throw new NotSupportedException($"Column '{name}' of type {typeof(TCol)} is not supported.");
        }
    }

    private static class ColumnBufferFactory
    {
        public static IColumnBuffer Create(Type propType, int capacity)
        {
            
            Type targetType = propType;

            if (propType.IsValueType && Nullable.GetUnderlyingType(propType) == null)
            {
                targetType = typeof(Nullable<>).MakeGenericType(propType);
            }

            var bufferType = typeof(ColumnBuffer<>).MakeGenericType(targetType);
            return (IColumnBuffer)Activator.CreateInstance(bufferType, capacity)!;
        }
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
    /// Create a DataFrame from a collection of Series.
    /// <para>
    /// This is the functional equivalent of the constructor <c>new DataFrame(series)</c>, 
    /// provided for consistency with other <c>From...</c> factory methods.
    /// </para>
    /// </summary>
    /// <param name="series">The series to combine into a DataFrame.</param>
    /// <returns>A new DataFrame containing the provided series.</returns>
    /// <exception cref="ArgumentException">Thrown if series have different lengths.</exception>
    /// <example>
    /// <code>
    /// var s1 = new Series("id", new[] { 1, 2, 3 });
    /// var s2 = new Series("name", new[] { "Alice", "Bob", "Charlie" });
    /// 
    /// var df = DataFrame.FromSeries(s1, s2);
    /// Console.WriteLine(df);
    /// </code>
    /// </example>
    public static DataFrame FromSeries(params Series[] series)
        => new(series);
    /// <summary>
    /// Create a DataFrame from a collection of Series.
    /// </summary>
    /// <param name="series">The series to combine.</param>
    public static DataFrame FromSeries(IEnumerable<Series> series)
        => new(series.ToArray());
    /// <summary>
    /// [High Performance] Stream data into Polars using Arrow C Stream Interface.
    /// This method supports datasets larger than available RAM by streaming chunks directly to Polars.
    /// </summary>
    /// <param name="data">Source data collection</param>
    /// <param name="batchSize">Rows per chunk (default 100,000)</param>
    /// <param name="providedSchema">Stream schema provided by user</param>
    public static DataFrame FromArrowStream<T>(IEnumerable<T> data, int batchSize = 100_000,Schema? providedSchema = null)
    {
        var schema = providedSchema ?? ArrowConverter.GetSchemaFromType<T>();
        var stream = data.ToArrowBatches(batchSize);

        var handle = ArrowStreamInterop.ImportEager(stream,schema);

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
        int rowsToShow = (int)Math.Min(Height, limit);
        using var previewDf = this.Head(rowsToShow);
        
        using var batch = ArrowFfiBridge.ExportDataFrame(previewDf.Handle);
        var schema = batch.Schema;

        var sb = new StringBuilder();
        
        sb.Append(@"
<style>
.pl-dataframe { font-family: 'Consolas', 'Monaco', monospace; font-size: 13px; border-collapse: collapse; border: 1px solid #e0e0e0; }
.pl-dataframe th { background-color: #f0f0f0; font-weight: bold; text-align: left; padding: 6px 12px; border-bottom: 2px solid #ccc; }
.pl-dataframe td { padding: 6px 12px; border-bottom: 1px solid #f0f0f0; white-space: pre; color: #333; }
.pl-dataframe tr:nth-child(even) { background-color: #f9f9f9; }
.pl-dataframe tr:hover { background-color: #f1f1f1; }
.pl-dtype { font-size: 10px; color: #999; display: block; margin-top: 2px; font-weight: normal; }
.pl-null { color: #d0d0d0; font-style: italic; }
.pl-dim { font-family: sans-serif; font-size: 12px; color: #666; margin-bottom: 8px; }
</style>");

        // Dimensions Info
        sb.Append($"<div class='pl-dim'>Polars DataFrame: <b>({Height} rows, {Width} columns)</b></div>");
        
        sb.Append("<div style='overflow-x:auto'><table class='pl-dataframe'>");

        // Header (From Arrow Schema)
        sb.Append("<thead><tr>");
        foreach (var field in schema.FieldsList)
        {
            var colName = System.Net.WebUtility.HtmlEncode(field.Name);
            var colType = field.DataType.Name; 
            
            sb.Append($"<th>{colName}<span class='pl-dtype'>{colType}</span></th>");
        }
        sb.Append("</tr></thead>");

        // Body (From Arrow Batch)
        sb.Append("<tbody>");
        
        int rowCount = batch.Length;
        int colCount = batch.ColumnCount;

        for (int r = 0; r < rowCount; r++)
        {
            sb.Append("<tr>");
            for (int c = 0; c < colCount; c++)
            {
                var colArray = batch.Column(c);
                var field = schema.GetFieldByIndex(c);
                
                string valStr = colArray.FormatValue(r);

                if (valStr != "null")
                {
                    if (field.DataType.TypeId == ArrowTypeId.Double)
                    {
                        if (double.TryParse(valStr, out double d)) 
                            valStr = d.ToString("G10");
                    }
                    else if (field.DataType.TypeId == ArrowTypeId.Float)
                    {
                        if (float.TryParse(valStr, out float f)) 
                            valStr = f.ToString("G7");
                    }
                }

                if (valStr == "null")
                {
                    sb.Append("<td class='pl-null'>null</td>");
                }
                else
                {
                    if (valStr.Length > 100) valStr = valStr[..97] + "...";
                    sb.Append($"<td>{System.Net.WebUtility.HtmlEncode(valStr)}</td>");
                }
            }
            sb.Append("</tr>");
        }
        
        // Footer for hidden rows
        long remaining = Height - rowsToShow;
        if (remaining > 0)
        {
             sb.Append($"<tr><td colspan='{colCount}' style='text-align:center; font-style:italic; color:#999; padding: 10px'>... {remaining} more rows ...</td></tr>");
        }

        sb.Append("</tbody></table></div>");

        return sb.ToString();
    }
}