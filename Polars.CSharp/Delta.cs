using System.Text;
using System.Text.Json;
using Polars.NET.Core;

namespace Polars.CSharp;
/// <summary>
/// Methods for DeltaLake 
/// </summary>
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
    /// <summary>
    /// Optimizes the layout of the Delta table by compacting small files (bin-packing) and optionally applying Z-Order clustering.
    /// <para>This operation improves read performance by reducing the number of files and co-locating related data.</para>
    /// </summary>
    /// <param name="path">The root URI of the Delta table.</param>
    /// <param name="targetSizeMb">The target file size in Megabytes for the compacted files (default: 128 MB).</param>
    /// <param name="partitionFilters">
    /// Optional dictionary of partition key-value pairs to restrict optimization to specific partitions.
    /// <para>Example: <c>new Dictionary&lt;string, string&gt; { { "date", "2024-01-01" } }</c></para>
    /// </param>
    /// <param name="zOrderColumns">
    /// Optional list of column names to apply Z-Order clustering. 
    /// <para>Z-Ordering co-locates data based on these columns, significantly speeding up queries that filter on them.</para>
    /// </param>
    /// <param name="cloudOptions">Cloud storage credentials and configuration.</param>
    /// <returns>The number of new data files created during the optimization process.</returns>
    /// <exception cref="ArgumentException">Thrown if the path is empty or target size is invalid.</exception>
    public static long Optimize(
        string path,
        long targetSizeMb = 128,
        Dictionary<string, string>? partitionFilters = null,
        IEnumerable<string>? zOrderColumns = null,
        CloudOptions? cloudOptions = null)
    {
        // 1. Validation
        if (string.IsNullOrWhiteSpace(path))
            throw new ArgumentException("Path cannot be empty.", nameof(path));

        if (targetSizeMb <= 0)
            throw new ArgumentException("Target size must be greater than 0 MB.", nameof(targetSizeMb));

        // 2. Prepare Parameters
        // Serialize partition filters to JSON string for the Rust backend
        string? filterJson = null;
        if (partitionFilters != null && partitionFilters.Count > 0)
        {
            filterJson = JsonSerializer.Serialize(partitionFilters);
        }

        // Convert Z-Order columns to array (Rust FFI expects string[] or null)
        string[]? zOrderColsArr = zOrderColumns?.ToArray();
        if (zOrderColsArr != null && zOrderColsArr.Length == 0)
        {
            zOrderColsArr = null;
        }

        // 3. Parse Cloud Options
        // Unlike Restore, Optimize needs all cloud retry/timeout settings passed down
        var (provider, retries, timeout, initBackoff, maxBackoff, cacheTtl, keys, values) = 
            CloudOptions.ParseCloudOptions(cloudOptions);

        // 4. Call Wrapper
        // The wrapper returns ulong (nuint), we cast to long for API consistency
        ulong result = PolarsWrapper.Optimize(
            path,
            targetSizeMb,
            filterJson,
            zOrderColsArr,
            provider.ToNative(),
            retries,
            timeout,
            initBackoff,
            maxBackoff,
            cacheTtl,
            keys,
            values
        );

        return (long)result;
    }
}