namespace Polars.FSharp

open Polars.NET.Core

/// <summary>
/// Common Delta Lake Table Features.
/// </summary>
module DeltaTableFeatures =

    /// <summary>
    /// Enables Deletion Vectors for efficient deletes/updates (Merge-on-Read).
    /// Requires Reader v3, Writer v7.
    /// </summary>
    [<Literal>]
    let DeletionVectors = "deletionVectors"

    /// <summary>
    /// Enables Change Data Feed (CDF) to track row-level changes.
    /// </summary>
    [<Literal>]
    let ChangeDataFeed = "changeDataFeed"

    /// <summary>
    /// Enables Column Mapping (allows renaming columns and special characters).
    /// </summary>
    [<Literal>]
    let ColumnMapping = "columnMapping"

    /// <summary>
    /// Enforces Append-Only retention (prevents deletes/updates).
    /// </summary>
    [<Literal>]
    let AppendOnly = "appendOnly"
    
    /// <summary>
    /// Enables Check Constraints on columns.
    /// </summary>
    [<Literal>]
    let CheckConstraints = "checkConstraints"

    /// <summary>
    /// Enables TimestampWithoutTimezone.
    /// </summary>
    [<Literal>]
    let TimestampWithoutTimezone = "TimestampWithoutTimezone"

    /// <summary>
    /// Enables IcebergCompatV1
    /// </summary>
    [<Literal>]
    let IcebergCompatV1 = "IcebergCompatV1"

/// <summary>
/// Common configuration keys for Delta Tables.
/// </summary>
module DeltaTableProperties =

    /// <summary>
    /// The shortest duration that deleted files are kept before Vacuum deletes them.
    /// Default: 7 days (e.g., "interval 7 days").
    /// </summary>
    [<Literal>]
    let DeletedFileRetentionDuration = "delta.deletedFileRetentionDuration"

    /// <summary>
    /// How long the history of the Delta Log is kept.
    /// Default: 30 days (e.g., "interval 30 days").
    /// </summary>
    [<Literal>]
    let LogRetentionDuration = "delta.logRetentionDuration"

    /// <summary>
    /// The target file size in bytes for Optimize/Bin-packing.
    /// Default: 134217728 (128 MB).
    /// </summary>
    [<Literal>]
    let TargetFileSize = "delta.targetFileSize"

    /// <summary>
    /// Whether to collect stats for columns (min/max/nulls).
    /// Default: true.
    /// </summary>
    [<Literal>]
    let DataSkippingNumIndexedCols = "delta.dataSkippingNumIndexedCols"
    
    /// <summary>
    /// If true, enables the Change Data Feed (CDF).
    /// </summary>
    [<Literal>]
    let EnableChangeDataFeed = "delta.enableChangeDataFeed"

/// <summary>
/// Methods for DeltaLake 
/// </summary>
type Delta =

    /// <summary>
    /// Represents a column from the incoming Source LazyFrame.
    /// (Internally maps to "{name}_src_tmp")
    /// </summary>
    static member Source(columnName: string) =
        pl.col $"{columnName}_src_tmp"

    /// <summary>
    /// Represents a column from the existing Target Delta Table.
    /// (Alias for standard Col(), adds semantic clarity)
    /// </summary>
    static member Target(columnName: string) =
        pl.col columnName

    /// <summary>
    /// Deletes rows from a Delta Lake table that match a given predicate.
    /// <para>
    /// By default, this operation performs a Copy-on-Write (CoW), meaning any underlying data files containing matching rows are entirely rewritten.
    /// </para>
    /// <para>
    /// Note: If Deletion Vectors are enabled on the table, this operation automatically shifts to a Merge-on-Read (MoR) approach. 
    /// Instead of expensive file rewrites, deleted rows are swiftly marked in a separate deletion vector file, which drastically improves deletion performance.
    /// </para>
    /// </summary>
    /// <param name="path">Path to the Delta table.</param>
    /// <param name="predicate">Filter expression to identify rows to delete.</param>
    /// <param name="cloudOptions">Cloud storage configuration.</param>
    static member Delete(
        path: string,
        predicate: Expr,
        ?cloudOptions: CloudOptions
    ) =
        // 1. Unpack Cloud Options
        let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
            CloudOptions.ParseCloudOptions cloudOptions

        // 2. Safely clone the expression handle
        use clonedPredicate = predicate.CloneHandle()

        // 3. Native Call
        PolarsWrapper.DeltaDelete(
            path,
            clonedPredicate,
            cProv,
            cRet,
            cToMs,
            cInitMs,
            cMaxMs,
            cCache,
            cKeys,
            cVals
        )

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
    static member Vacuum(
        path: string,
        ?retentionHours: int,
        ?enforceRetention: bool,
        ?dryRun: bool,
        ?vacuumModeFull: bool,
        ?cloudOptions: CloudOptions
    ) : int64 =
        let pRetention = defaultArg retentionHours -1
        
        if retentionHours.IsSome && retentionHours.Value < 0 then
            invalidArg "retentionHours" "Retention hours cannot be negative."

        let pEnforce = defaultArg enforceRetention true
        let pDryRun = defaultArg dryRun false
        let pFull = defaultArg vacuumModeFull false

        // Vacuum doesn't need full retry mechanisms, just auth keys
        let _, _, _, _, _, _, cKeys, cVals = CloudOptions.ParseCloudOptions cloudOptions

        PolarsWrapper.Vacuum(
            path,
            pRetention,
            pEnforce,
            pDryRun,
            pFull,
            cKeys,
            cVals
        )

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
    static member Restore(
        path: string,
        ?version: int64,
        ?timestamp: System.DateTime,
        ?ignoreMissingFiles: bool,
        ?protocolDowngradeAllowed: bool,
        ?cloudOptions: CloudOptions
    ) : int64 =
        // 1. Validation: Version and Timestamp are mutually exclusive
        if version.IsSome && timestamp.IsSome then
            invalidArg "version/timestamp" "Cannot specify both 'version' and 'timestamp' for Restore."
        
        if version.IsNone && timestamp.IsNone then
            invalidArg "version/timestamp" "Must specify either 'version' or 'timestamp' for Restore."

        // 2. Prepare Parameters
        let targetVer = defaultArg version -1L
        
        let targetTs = 
            match timestamp with
            | Some dt -> 
                let utcTime = dt.ToUniversalTime()
                System.DateTimeOffset(utcTime).ToUnixTimeMilliseconds()
            | None -> -1L

        let pIgnore = defaultArg ignoreMissingFiles false
        let pDowngrade = defaultArg protocolDowngradeAllowed false

        // 3. Parse Cloud Options
        let _, _, _, _, _, _, cKeys, cVals = CloudOptions.ParseCloudOptions cloudOptions

        // 4. Call Wrapper
        PolarsWrapper.Restore(
            path,
            targetVer,
            targetTs,
            pIgnore,
            pDowngrade,
            cKeys,
            cVals
        )
    /// <summary>
    /// Returns provenance information, including the operation, user, timestamp, etc., for each write to a table.
    /// </summary>
    /// <param name="path">The root URI of the Delta table.</param>
    /// <param name="limit">The number of latest commits to retrieve. Set to 0 (default) to retrieve all history.</param>
    /// <param name="cloudOptions">Cloud storage credentials.</param>
    /// <returns>A DataFrame containing the commit history.</returns>
    static member History(
        path: string,
        ?limit: int,
        ?cloudOptions: CloudOptions
    ) : DataFrame =
        let pLimit = defaultArg limit 0
        let _, _, _, _, _, _, cKeys, cVals = CloudOptions.ParseCloudOptions cloudOptions
        
        // Fetch JSON string from Rust core
        let json = PolarsWrapper.History(path, pLimit, cKeys, cVals)
        let buffer = System.Text.Encoding.UTF8.GetBytes json
        
        let mutable df = DataFrame.ReadJson(buffer, jsonFormat = JsonFormat.Json, inferSchemaLen = 2000UL)

        // =========================================================
        // Post-Processing
        // =========================================================
        let cols = df.ColumnNames |> Seq.toList

        // i64 -> Datetime
        if Seq.contains "timestamp" cols then
            let expr = pl.col("timestamp").Cast(DataType.Datetime(TimeUnit.Milliseconds, Some "UCT")).Alias "timestamp"
            df <- df.WithColumns [ expr :> IColumnExpr ]

        // operationParameters (Struct -> Columns)
        if Seq.contains "operationParameters" cols then
            df <- df.UnnestColumn "operationParameters"

        // Sort by version/timestamp
        if Seq.contains "version" cols then
            df <- df.Sort("version", descending = true)
        else
            df <- df.Sort("timestamp", descending = true)

        // Reorder columns to put priority info first
        let priorityCols = [| "version"; "timestamp"; "operation"; "mode"; "predicate"; "userName" |]
        let currentCols = df.ColumnNames |> Seq.toArray
        
        let selection = 
            priorityCols 
            |> Array.filter (fun c -> Array.contains c currentCols)
        
        let rest = 
            currentCols 
            |> Array.filter (fun c -> not (Array.contains c priorityCols))
            
        let finalCols = Array.append selection rest
        
        df.Select(finalCols |> Array.map pl.col |> Array.map (fun x -> x :> IColumnExpr))

    /// <summary>
    /// Optimizes the layout of the Delta table by compacting small files (bin-packing) and optionally applying Z-Order clustering.
    /// <para>
    /// This operation significantly improves read performance by reducing the number of files and co-locating related data.
    /// </para>
    /// <para>
    /// Note: If Deletion Vectors (DV) are enabled on the table, any soft-deleted rows tracked by the vectors 
    /// will be physically removed (materialized) from the newly compacted Parquet files, effectively clearing 
    /// the deletion vectors for the optimized partitions.
    /// </para>
    /// </summary>
    /// <param name="path">The root URI of the Delta table.</param>
    /// <param name="targetSizeMb">The target file size in Megabytes for the compacted files (default: 128 MB).</param>
    /// <param name="partitionFilters">
    /// Optional sequence of partition key-value pairs to restrict optimization to specific partitions.
    /// <para>Example: <c>[ ("date", "2024-01-01") ]</c></para>
    /// </param>
    /// <param name="zOrderColumns">
    /// Optional sequence of column names to apply Z-Order clustering. 
    /// <para>Z-Ordering co-locates data based on these columns, significantly speeding up queries that filter on them.</para>
    /// </param>
    /// <param name="cloudOptions">Cloud storage credentials and configuration.</param>
    /// <returns>The number of new data files created during the optimization process.</returns>
    static member Optimize(
        path: string,
        ?targetSizeMb: int64,
        ?partitionFilters: seq<string * string>,
        ?zOrderColumns: seq<string>,
        ?cloudOptions: CloudOptions
    ) : int64 =
        // 1. Validation
        if System.String.IsNullOrWhiteSpace path then
            invalidArg "path" "Path cannot be empty."

        let pTargetSize = defaultArg targetSizeMb 128L
        if pTargetSize <= 0L then
            invalidArg "targetSizeMb" "Target size must be greater than 0 MB."

        // 2. Prepare Parameters
        let filterJson = 
            match partitionFilters with
            | Some filters -> 
                let d = dict filters // Convert F# seq to IDictionary
                if d.Count > 0 then System.Text.Json.JsonSerializer.Serialize(d) else null
            | None -> null

        let zOrderColsArr = 
            match zOrderColumns with
            | Some cols -> 
                let arr = cols |> Seq.toArray
                if arr.Length > 0 then arr else null
            | None -> null

        // 3. Parse Cloud Options
        let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
            CloudOptions.ParseCloudOptions cloudOptions

        // 4. Call Wrapper
        let result = PolarsWrapper.Optimize(
            path,
            pTargetSize,
            filterJson,
            zOrderColsArr,
            cProv,
            cRet,
            cToMs,
            cInitMs,
            cMaxMs,
            cCache,
            cKeys,
            cVals
        )

        int64 result
    /// <summary>
    /// Enables a specific feature on the Delta Table.
    /// <para>
    /// WARNING: Enabling features may upgrade the Delta Protocol version (e.g., to Reader v3 / Writer v7).
    /// Older readers may not be able to read the table after this operation.
    /// </para>
    /// </summary>
    /// <param name="path">The root URI of the Delta table.</param>
    /// <param name="featureName">The name of the feature to enable (use <see cref="DeltaTableFeatures"/> constants).</param>
    /// <param name="allowProtocolIncrease">
    /// If true, allows the operation to upgrade the table's Protocol Version if the feature requires it.
    /// If false, and the current protocol is too low, the operation will fail.
    /// </param>
    /// <param name="cloudOptions">Cloud storage credentials.</param>
    static member AddFeature(
        path: string,
        featureName: string,
        ?allowProtocolIncrease: bool,
        ?cloudOptions: CloudOptions
    ) =
        let pAllow = defaultArg allowProtocolIncrease true
        
        // Configuration updates only need cloud auth credentials
        let _, _, _, _, _, _, cKeys, cVals = CloudOptions.ParseCloudOptions cloudOptions

        PolarsWrapper.AddFeature(
            path,
            featureName,
            pAllow,
            cKeys,
            cVals
        )

    /// <summary>
    /// Sets or updates properties on the Delta Table.
    /// </summary>
    /// <param name="path">The root URI of the Delta table.</param>
    /// <param name="properties">A sequence of key-value pairs to set.</param>
    /// <param name="raiseIfNotExists">
    /// If true, the operation fails if you try to update a property that doesn't strictly exist (rarely used, default false).
    /// </param>
    /// <param name="cloudOptions">Cloud storage credentials.</param>
    static member SetTableProperties(
        path: string,
        properties: seq<string * string>,
        ?raiseIfNotExists: bool,
        ?cloudOptions: CloudOptions
    ) =
        let propsArr = properties |> Seq.toArray
        if propsArr.Length = 0 then
            invalidArg "properties" "Properties sequence cannot be empty."

        // Elegantly unzip the array of tuples into two separate arrays of keys and values
        let propKeys, propValues = Array.unzip propsArr

        let pRaise = defaultArg raiseIfNotExists false
        
        // Configuration updates only need cloud auth credentials
        let _, _, _, _, _, _, cKeys, cVals = CloudOptions.ParseCloudOptions cloudOptions

        PolarsWrapper.SetTableProperties(
            path,
            propKeys,
            propValues,
            pRaise,
            cKeys,
            cVals
        )

/// <summary>
/// A builder for constructing ordered Delta Lake Merge operations in F#.
/// This ensures that matched and not-matched conditions are evaluated exactly 
/// in the order they are chained by the user, adhering to standard SQL MERGE semantics.
/// </summary>
type DeltaMergeBuilder (sourceLf: LazyFrame, path: string, mergeKeys: string array, canEvolve: bool, cloudOptions: CloudOptions option) =
    
    // ResizeArray is F#'s idiomatic alias for System.Collections.Generic.List
    let actions = ResizeArray<MergeActionType * Expr>()

    /// <summary>
    /// Update the matched target row with source data.
    /// </summary>
    member this.WhenMatchedUpdate(?condition: Expr) =
        // If condition is None, default to pl.lit true
        actions.Add(MergeActionType.MatchedUpdate, defaultArg condition (pl.lit true))
        this

    /// <summary>
    /// Delete the matched target row.
    /// </summary>
    member this.WhenMatchedDelete(?condition: Expr) =
        actions.Add(MergeActionType.MatchedDelete, defaultArg condition (pl.lit true))
        this

    /// <summary>
    /// Insert a new row from the source data when it does not match any target row.
    /// </summary>
    member this.WhenNotMatchedInsert(?condition: Expr) =
        actions.Add(MergeActionType.NotMatchedInsert, defaultArg condition (pl.lit true))
        this

    /// <summary>
    /// Delete the target row when it does not exist in the source data.
    /// </summary>
    member this.WhenNotMatchedBySourceDelete(?condition: Expr) =
        actions.Add(MergeActionType.NotMatchedBySourceDelete, defaultArg condition (pl.lit true))
        this

    /// <summary>
    /// Executes the constructed merge operation against the Delta Table.
    /// </summary>
    member this.Execute() =
        if actions.Count = 0 then
            this.WhenMatchedUpdate() |> ignore
            this.WhenNotMatchedInsert() |> ignore

        let actionTypes = 
            actions |> Seq.map (fun (t, _) -> t.ToNative()) |> Seq.toArray
        
        let actionExprs = 
            actions |> Seq.map (fun (_, expr) -> expr.CloneHandle()) |> Seq.toArray

        let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
            CloudOptions.ParseCloudOptions cloudOptions

        PolarsWrapper.DeltaMergeOrdered(
            sourceLf.CloneHandle(), 
            path,
            mergeKeys,
            actionTypes,
            actionExprs,
            canEvolve,
            cProv,
            cRet,
            cToMs,
            cInitMs,
            cMaxMs,
            cCache,
            cKeys,
            cVals
        )