#pragma warning disable CS1591
using System.Text;
using Polars.NET.Core;
using Polars.NET.Core.Helpers;
namespace Polars.CSharp;

/// <summary>
/// Polars Static Helpers
/// </summary>
public static class Polars
{
    /// <summary>
    /// Column Expr (name: string)
    /// </summary>
    /// <param name="name"></param>
    /// <returns></returns>
    public static Expr Col(string name)
        => new(PolarsWrapper.Col(name));
    /// <summary>
    /// Column Exprs (name: string)
    /// </summary>
    /// <param name="names"></param>
    /// <returns></returns>
    public static Expr Col(params string[] names)
        => new(PolarsWrapper.Cols(names));
    /// <summary>
    /// Return the lines count of current context.
    /// </summary>
    public static Expr Len()
        => new(PolarsWrapper.Len());
    // --- Literals ---
    public static Expr Lit(string? value)
    {
        if (value is null)
        {
            return new Expr(PolarsWrapper.LitNull());
        }
        return new Expr(PolarsWrapper.Lit(value));
    }
    public static Expr Lit(sbyte value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(byte value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(short value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(ushort value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(int value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(uint value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(long value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(ulong value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(Int128 value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(double value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(DateTime value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(DateTimeOffset value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(DateOnly value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(TimeOnly value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(TimeSpan value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(bool value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(Half value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(float value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(decimal value) => new(PolarsWrapper.Lit(value));
    public static Expr LitNull() => new(PolarsWrapper.LitNull());
    /// <summary>
    /// Convert Series into Literal Expr.
    /// <para>The Series is cloned implicitly, so the original Series remains valid.</para>
    /// </summary>
    public static Expr Lit(Series series)
    {
        var clonedHandle = PolarsWrapper.CloneSeries(series.Handle);
        return new Expr(PolarsWrapper.Lit(clonedHandle));
    }
    // -------------------------------------------------------------------------
    // Struct Literals
    // -------------------------------------------------------------------------

    /// <summary>
    /// Create a Struct Expression from a single anonymous object or class instance.
    /// <para>Example: <c>LitStruct(new { A = 1, B = "hi" })</c></para>
    /// </summary>
    public static Expr LitStruct<T>(T value) where T : class
        => LitStruct([value]);

    /// <summary>
    /// Create a Struct Expression from an array of objects.
    /// <para>The properties of the objects become the fields of the struct.</para>
    /// </summary>
    public static Expr LitStruct<T>(T[] values)
    {
        SeriesHandle sHandle = StructPacker.Pack("literal", values);
        ExprHandle eHandle = PolarsWrapper.Lit(sHandle);
        return new Expr(eHandle);
    }
    // =========================================================================
    // The "Magic" Lit
    // =========================================================================

    public static Expr Lit<T>(T[] values)
    {
        using var s = Series.From("", values);
        
        return Lit(s);
    }
    
    public static Expr Lit<T>(IEnumerable<T> values)
    {
        if (values is T[] arr) return Lit(arr);
        return Lit(values.ToArray());
    }
    
    // ---------------------------------------------------------
    // Selectors Entry Points
    // ---------------------------------------------------------

    /// <summary>
    /// String matching selectors namespace.
    /// Usage: Polars.Selectors.StartsWith("A")
    /// </summary>
    public static class Selectors
    {
        /// <summary>
        /// Select all columns.
        /// </summary>
        public static Selector All() 
            => new(PolarsWrapper.SelectorAll());

        /// <summary>
        /// Select all numeric columns (Int, Float, etc.).
        /// </summary>
        public static Selector Numeric() 
            => new(PolarsWrapper.SelectorNumeric());

        /// <summary>
        /// Select all string/utf8 columns.
        /// </summary>
        public static Selector String() 
            => new(PolarsWrapper.SelectorByDtype(PlDataType.String));

        /// <summary>
        /// Select all date columns.
        /// </summary>
        public static Selector Date() 
            => new(PolarsWrapper.SelectorByDtype(PlDataType.Date));

        /// <summary>
        /// Select all datetime columns (any precision/timezone).
        /// </summary>
        public static Selector Datetime() 
            => new(PolarsWrapper.SelectorByDtype(PlDataType.Datetime));

        /// <summary>
        /// Select columns by specific DataType.
        /// </summary>
        public static Selector DType(DataType type) 
        {
            var typeKind = type.Kind;
            return new(PolarsWrapper.SelectorByDtype(typeKind.ToNative()));
        }
        /// <summary>
        /// Select column whose name starts with given prefix.
        /// </summary>
        /// <param name="prefix"></param>
        /// <returns></returns>
        public static Selector StartsWith(string prefix) 
            => new(PolarsWrapper.SelectorStartsWith(prefix));
        /// <summary>
        /// Select column whose name ends with given suffix.
        /// </summary>
        /// <param name="suffix"></param>
        /// <returns></returns>
        public static Selector EndsWith(string suffix) 
            => new(PolarsWrapper.SelectorEndsWith(suffix));
        /// <summary>
        /// Select column whose name contains given string.
        /// </summary>
        /// <param name="str"></param>
        /// <returns></returns>
        public static Selector Contains(string str) 
            => new(PolarsWrapper.SelectorContains(str));
        /// <summary>
        /// Select column whose name matches given string.
        /// </summary>
        /// <param name="regex"></param>
        /// <returns></returns>
        public static Selector Matches(string regex) 
            => new(PolarsWrapper.SelectorMatch(regex));
    }
    // ==========================================
    // Control Flow
    // ==========================================

    /// <summary>
    /// If-Else control flow: if predicate evaluates to true, return trueExpr, otherwise return falseExpr.
    /// Similar to SQL's CASE WHEN ... THEN ... ELSE ... END.
    /// </summary>
    public static Expr IfElse(Expr predicate, Expr trueExpr, Expr falseExpr)
    {
        var p = PolarsWrapper.CloneExpr(predicate.Handle);
        var t = PolarsWrapper.CloneExpr(trueExpr.Handle);
        var f = PolarsWrapper.CloneExpr(falseExpr.Handle);
        
        return new Expr(PolarsWrapper.IfElse(p, t, f));
    }
    // ==========================================
    // List Operations
    // ==========================================
    /// <summary>
    /// Concat multiple list expressions into a single list expression.
    /// </summary>
    public static Expr ConcatList(params Expr[] exprs)
    {
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        return new Expr(PolarsWrapper.ConcatList(handles));
    }
    // ==========================================
    // Struct Operations
    // ==========================================

    /// <summary>
    /// Combine multiple expressions into a Struct expression.
    /// </summary>
    public static Expr AsStruct(params Expr[] exprs)
    {
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        return new Expr(PolarsWrapper.AsStruct(handles));
    }
    // ==========================================
    // SQL Context
    // ==========================================
    /// <summary>
    /// Create a new SQL Context.
    /// </summary>
    public static SqlContext Sql() => new();

    // ==========================================
    // Temporal
    // ==========================================

    /// <summary>
    /// Combine a Date expression and a Time expression into a Datetime expression.
    /// <para>
    /// Useful for combining a Date column with a Time column, or combining literal arrays.
    /// </para>
    /// <para>
    /// <b>Note:</b> Only sub-second units (<see cref="TimeUnit.Nanoseconds"/>, <see cref="TimeUnit.Microseconds"/>, <see cref="TimeUnit.Milliseconds"/>) are supported.
    /// </para>
    /// </summary>
    /// <param name="date">Expression for the Date component (can be a Column, Literal, or calculation).</param>
    /// <param name="time">Expression for the Time component.</param>
    /// <param name="tu">The desired TimeUnit for the resulting Datetime (default: Microseconds).</param>
    /// <returns>A new expression evaluating to Datetime.</returns>
    /// <example>
    /// <code>
    /// // 1. Combine Columns
    /// df.Select(Polars.Combine(Col("date_col"), Col("time_col")));
    /// 
    /// // 2. Combine Arrays (Literals)
    /// var dates = new[] { new DateOnly(2024, 1, 1), new DateOnly(2024, 1, 2) };
    /// var times = new[] { new TimeOnly(10, 0), new TimeOnly(11, 0) };
    /// 
    /// df.Select(
    ///     Polars.Combine(Polars.Lit(dates), Polars.Lit(times)).Alias("combined")
    /// );
    /// </code>
    /// </example>
    public static Expr CombineDateAndTime(Expr date, Expr time, TimeUnit tu = TimeUnit.Microseconds)
        => date.Dt.Combine(time, tu);
    /// <summary>
    /// Delete rows from a Delta Lake table based on a predicate.
    /// This operation performs a Copy-on-Write: files containing matching rows are rewritten.
    /// </summary>
    /// <param name="path">Path to the Delta table.</param>
    /// <param name="predicate">Filter expression to identify rows to delete.</param>
    /// <param name="cloudOptions">Cloud storage configuration.</param>
    public static void DeleteDelta(
        string path,
        Expr predicate,
        CloudOptions? cloudOptions = null)
    {
        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        using var clonedPredicate = predicate.CloneHandle();

        PolarsWrapper.DeltaDelete(
            path,
            clonedPredicate,
            provider.ToNative(),
            retries,
            retryTimeoutMs,
            retryInitBackoffMs,
            retryMaxBackoffMs,
            cacheTtl,
            keys,
            values
        );
    }

}

public static class Delta
{
    /// <summary>
    /// Represents a column from the incoming Source LazyFrame.
    /// (Internally maps to "{name}_src_tmp")
    /// </summary>
    public static Expr Source(string columnName)
    {
        return Polars.Col($"{columnName}_src_tmp");
    }

    /// <summary>
    /// Represents a column from the existing Target Delta Table.
    /// (Alias for standard Col(), adds semantic clarity)
    /// </summary>
    public static Expr Target(string columnName)
    {
        return Polars.Col(columnName);
    }
    /// <summary>
    /// Recursively delete files and directories in the table that are no longer needed by the table for maintaining the transaction history.
    /// </summary>
    /// <param name="path">The root URI of the Delta table (e.g., "s3://bucket/table").</param>
    /// <param name="retentionHours">
    /// The retention threshold in hours. Files removed before this threshold will be deleted. 
    /// Set to -1 to use the default retention period configured in the table (usually 168 hours / 7 days).
    /// </param>
    /// <param name="enforceRetention">
    /// If true, prevents the vacuum command from deleting files that have not yet reached the retention threshold. 
    /// Setting this to false allows forcing deletion of recent files (use with caution).
    /// </param>
    /// <param name="dryRun">
    /// If true, simply returns the number of files that would be deleted, without actually deleting them.
    /// </param>
    /// <param name="vacuumModeFull">
    /// Specifies the vacuum mode:
    /// <para>false (Lite): Only removes files that are explicitly referenced as removed in the transaction log. Faster.</para>
    /// <para>true (Full): Scans the entire storage directory to find and remove any files not referenced in the transaction log (including orphan files from failed writes). Slower but more thorough.</para>
    /// </param>
    /// <param name="cloudOptions">Cloud storage credentials and configuration (e.g. AWS Keys, Azure Secrets).</param>
    /// <returns>The number of files deleted (or selected for deletion in dry-run mode).</returns>
    public static long Vacuum(
        string path,
        int retentionHours = -1,
        bool enforceRetention = true,
        bool dryRun = false,
        bool vacuumModeFull = false, 
        CloudOptions? cloudOptions = null)
    {
        var (_, _, _, _, _, _, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        return PolarsWrapper.Vacuum(
            path,
            retentionHours,
            enforceRetention,
            dryRun,
            vacuumModeFull,
            keys,
            values
        );
    }
    /// <summary>
    /// Restores the Delta table to a previous state defined by a specific version or timestamp.
    /// <para>Note: This operation creates a new commit (version) that reflects the state of the table at the target point.</para>
    /// </summary>
    /// <param name="path">The root URI of the Delta table.</param>
    /// <param name="version">The specific version number to restore to. Set to null if restoring by timestamp.</param>
    /// <param name="timestamp">The timestamp to restore to. Set to null if restoring by version.</param>
    /// <param name="ignoreMissingFiles">
    /// If true, the restore operation will ignore missing files (e.g., those deleted by Vacuum). 
    /// If false, it will fail if any required files are missing.
    /// </param>
    /// <param name="protocolDowngradeAllowed">Whether to allow downgrading the Delta Protocol version during restore (rarely needed).</param>
    /// <param name="cloudOptions">Cloud storage credentials.</param>
    /// <returns>The new version number of the table after the restore operation.</returns>
    /// <exception cref="ArgumentException">Thrown if neither or both version and timestamp are provided.</exception>
    public static long Restore(
        string path,
        long? version = null,
        DateTime? timestamp = null,
        bool ignoreMissingFiles = false,
        bool protocolDowngradeAllowed = false,
        CloudOptions? cloudOptions = null)
    {
        // 1. Validation: Version and Timestamp are mutually exclusive
        if (version.HasValue && timestamp.HasValue)
            throw new ArgumentException("Cannot specify both 'version' and 'timestamp' for Restore.");

        if (!version.HasValue && !timestamp.HasValue)
            throw new ArgumentException("Must specify either 'version' or 'timestamp' for Restore.");

        // 2. Prepare Parameters
        // Rust uses -1 to indicate "not set"
        long targetVer = version ?? -1;
        long targetTs = -1;

        if (timestamp.HasValue)
        {
            // Convert DateTime to Unix Milliseconds
            DateTime utcTime = timestamp.Value.ToUniversalTime();
            targetTs = new DateTimeOffset(utcTime).ToUnixTimeMilliseconds();
        }

        // 3. Parse Cloud Options
        // We only need keys/values for the Delta Lake object store
        var (_, _, _, _, _, _, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        // 4. Call Wrapper
        return PolarsWrapper.Restore(
            path,
            targetVer,
            targetTs,
            ignoreMissingFiles,
            protocolDowngradeAllowed,
            keys,
            values
        );
    }
    /// <summary>
    /// Returns provenance information, including the operation, user, timestamp, etc., for each write to a table.
    /// </summary>
    /// <param name="path">The root URI of the Delta table.</param>
    /// <param name="limit">The number of latest commits to retrieve. Set to 0 (default) to retrieve all history.</param>
    /// <param name="cloudOptions">Cloud storage credentials.</param>
    /// <returns>A DataFrame containing the commit history.</returns>
    public static DataFrame History(
        string path,
        int limit = 0,
        CloudOptions? cloudOptions = null)
    {
        var (_, _, _, _, _, _, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);
        string json = PolarsWrapper.History(path, limit, keys, values);
        byte[] buffer = Encoding.UTF8.GetBytes(json);
        
        var df = DataFrame.ReadJson(buffer, jsonFormat: JsonFormat.Json, inferSchemaLen: 2000);

        // =========================================================
        // Post-Processing
        // =========================================================

        // i64 -> Datetime
        if (df.ColumnNames.Contains("timestamp"))
        {
            df = df.WithColumns(
                Polars.Col("timestamp")
                    .Cast(DataType.Datetime(TimeUnit.Milliseconds,"UCT")) 
                    .Alias("timestamp") 
            );
        }

        // operationParameters (Struct -> Columns)
        if (df.ColumnNames.Contains("operationParameters"))
        {
            df = df.Unnest("operationParameters");
        }

        if (df.ColumnNames.Contains("version"))
        {
            df = df.Sort("version", descending: true);
        }
        else
        {
            df = df.Sort("timestamp", descending: true);
        }

        var priorityCols = new[] { "version", "timestamp", "operation", "mode", "predicate", "userName" };
        var existingCols = df.ColumnNames;
        var selection = priorityCols.Where(c => existingCols.Contains(c)).ToList();
        
        selection.AddRange(existingCols.Except(priorityCols));
        
        return df.Select(selection.Select(c => Polars.Col(c)).ToArray());
    }
}