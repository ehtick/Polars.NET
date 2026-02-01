using Apache.Arrow;
using Apache.Arrow.C;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Native;

namespace Polars.NET.Core;

public static partial class PolarsWrapper
{
    public static unsafe DataFrameHandle ReadCsv(
        string path,
        // 1. Columns Projection
        string[]? columns, 
        // 2. Schema Handle
        SchemaHandle? schema, 
        // 3. Configs
        bool hasHeader,
        char separator,
        bool ignoreErrors,
        bool tryParseDates,
        bool lowMemory,
        ulong skipRows,
        ulong? nRows,
        ulong? inferSchemaLength,
        PlEncoding encoding
    )
    {
        // --- Optional Sizes ---
        UIntPtr nRowsVal = nRows.HasValue ? (UIntPtr)nRows.Value : 0;
        UIntPtr* pNRows = nRows.HasValue ? &nRowsVal : null;

        UIntPtr inferVal = inferSchemaLength.HasValue ? (UIntPtr)inferSchemaLength.Value : 0;
        UIntPtr* pInfer = inferSchemaLength.HasValue ? &inferVal : null;

        // --- Prepare Handle ---
        SchemaHandle schemaHandle = schema ?? new SchemaHandle();

        // --- Execute ---
        return UseUtf8StringArray(columns, (colPtrs) =>
        {
            return ErrorHelper.Check(NativeBindings.pl_read_csv(
                path,
                
                // Columns
                colPtrs,
                (UIntPtr)colPtrs.Length,

                // Configs
                hasHeader,
                (byte)separator,
                ignoreErrors,
                tryParseDates,
                lowMemory,

                // Sizes
                (UIntPtr)skipRows,
                pNRows,
                pInfer,

                // Schema & Encoding
                schemaHandle, 
                encoding
            ));
        });
    }
    public static Task<DataFrameHandle> ReadCsvAsync(
        string path,
        string[]? columns, 
        // 2. Schema Handle
        SchemaHandle? schema, 
        // 3. Configs
        bool hasHeader,
        char separator,
        bool ignoreErrors,
        bool tryParseDates,
        bool lowMemory,
        ulong skipRows,
        ulong? nRows,
        ulong? inferSchemaLength,
        PlEncoding encoding)
    {
        return Task.Run(() => ReadCsv(
        path,
        columns,
        schema,
        hasHeader,
        separator,
        ignoreErrors,
        tryParseDates,
        lowMemory,
        skipRows,
        nRows,
        inferSchemaLength,
        encoding
        ));
    }
    public static unsafe LazyFrameHandle ScanCsv(
        string path,
        SchemaHandle? schema,
        bool hasHeader,
        char separator,
        bool ignoreErrors,
        bool tryParseDates,
        bool lowMemory,
        bool cache,
        bool rechunk,
        ulong skipRows,
        ulong? nRows,
        ulong? inferSchemaLength,
        string? rowIndexName,
        ulong rowIndexOffset,
        PlEncoding encoding)
    {
        UIntPtr nRowsVal = nRows.HasValue ? (UIntPtr)nRows.Value : 0;
        UIntPtr* pNRows = nRows.HasValue ? &nRowsVal : null;

        UIntPtr inferVal = inferSchemaLength.HasValue ? (UIntPtr)inferSchemaLength.Value : 0;
        UIntPtr* pInfer = inferSchemaLength.HasValue ? &inferVal : null;

        SchemaHandle schemaHandle = schema ?? new SchemaHandle();

        return ErrorHelper.Check(NativeBindings.pl_scan_csv(
            path,
            hasHeader,
            (byte)separator,
            ignoreErrors,
            tryParseDates,
            lowMemory,
            cache,
            rechunk,
            (UIntPtr)skipRows,
            pNRows,
            pInfer,
            rowIndexName, 
            (UIntPtr)rowIndexOffset,
            schemaHandle,
            encoding
        ));
    }
    public static unsafe LazyFrameHandle ScanCsv(
        byte[] buffer,
        SchemaHandle? schema,
        bool hasHeader,
        char separator,
        bool ignoreErrors,
        bool tryParseDates,
        bool lowMemory,
        bool cache,
        bool rechunk,
        ulong skipRows,
        ulong? nRows,
        ulong? inferSchemaLength,
        string? rowIndexName,
        ulong rowIndexOffset,
        PlEncoding encoding)
    {
        if (buffer == null || buffer.Length == 0)
            throw new ArgumentException("Buffer cannot be empty", nameof(buffer));

        UIntPtr nRowsVal = nRows.HasValue ? (UIntPtr)nRows.Value : 0;
        UIntPtr* pNRows = nRows.HasValue ? &nRowsVal : null;

        UIntPtr inferVal = inferSchemaLength.HasValue ? (UIntPtr)inferSchemaLength.Value : 0;
        UIntPtr* pInfer = inferSchemaLength.HasValue ? &inferVal : null;

        SchemaHandle schemaHandle = schema ?? new SchemaHandle();

        fixed (byte* pBuffer = buffer)
        {
            return ErrorHelper.Check(NativeBindings.pl_scan_csv_mem(
                pBuffer,
                (UIntPtr)buffer.Length,
                
                hasHeader,
                (byte)separator,
                ignoreErrors,
                tryParseDates,
                lowMemory,
                cache,
                rechunk,
                (UIntPtr)skipRows,
                pNRows,
                pInfer,
                rowIndexName,
                (UIntPtr)rowIndexOffset,
                schemaHandle,
                encoding
            ));
        }
    }

    public static DataFrameHandle ReadParquet(
        string path,
        string[] columns,
        ulong? nRows,         
        PlParallelStrategy parallel,
        bool lowMemory,
        string? rowIndexName,
        uint rowIndexOffset)
    {
        return UseUtf8StringArray(columns, colPtrs =>
        {
            unsafe
            {
                ulong limitVal = nRows.GetValueOrDefault();
                IntPtr limitPtr = nRows.HasValue ? (IntPtr)(&limitVal) : IntPtr.Zero;

                var h = NativeBindings.pl_read_parquet(
                    path,
                    colPtrs, (UIntPtr)columns.Length,
                    limitPtr,
                    parallel,
                    lowMemory,
                    rowIndexName,
                    rowIndexOffset
                );

                return ErrorHelper.Check(h);
            }
        });
    }
    public static DataFrameHandle ReadParquet(
        byte[] buffer,       
        string[] columns,
        ulong? nRows,
        PlParallelStrategy parallel,
        bool lowMemory,
        string? rowIndexName,
        uint rowIndexOffset)
    {
        return UseUtf8StringArray(columns, colPtrs =>
        {
            unsafe
            {
                fixed (byte* pBuffer = buffer)
                {
                    ulong limitVal = nRows.GetValueOrDefault();
                    IntPtr limitPtr = nRows.HasValue ? (IntPtr)(&limitVal) : IntPtr.Zero;

                    var h = NativeBindings.pl_read_parquet_memory(
                        (IntPtr)pBuffer, (UIntPtr)buffer.Length,
                        colPtrs, (UIntPtr)columns.Length,
                        limitPtr,
                        parallel,
                        lowMemory,
                        rowIndexName,
                        rowIndexOffset
                    );

                    return ErrorHelper.Check(h);
                }
            }
        });
    }
    public static Task<DataFrameHandle> ReadParquetAsync(
        string path,
        string[] columns, 
        ulong? nRows,   
        PlParallelStrategy parallel, 
        bool lowMemory, 
        string? rowIndexName, 
        uint rowIndexOffset)
    {
        return Task.Run(() => ReadParquet(path,columns,nRows,parallel,lowMemory,rowIndexName,rowIndexOffset));
    }
    public static LazyFrameHandle ScanParquet(
        string path,
        ulong? nRows,
        PlParallelStrategy parallel,
        bool lowMemory,
        bool useStatistics,
        bool glob,
        bool allowMissingColumns,
        string? rowIndexName,
        uint rowIndexOffset,
        string? includePathColumn,
        SchemaHandle? schema,              
        SchemaHandle? hivePartitionSchema, 
        bool tryParseHiveDates)
    {
        unsafe
        {
            ulong nRowsVal = nRows.GetValueOrDefault();
            IntPtr nRowsPtr = nRows.HasValue ? (IntPtr)(&nRowsVal) : IntPtr.Zero;

            using var schemaLock = new SafeHandleLock<SchemaHandle>(
                schema != null ? [schema] : null
            );
            IntPtr schemaPtr = schema != null ? schemaLock.Pointers[0] : IntPtr.Zero;

            using var hiveLock = new SafeHandleLock<SchemaHandle>(
                hivePartitionSchema != null ? [hivePartitionSchema] : null
            );
            IntPtr hiveSchemaPtr = hivePartitionSchema != null ? hiveLock.Pointers[0] : IntPtr.Zero;

            var h = NativeBindings.pl_scan_parquet(
                path, 
                nRowsPtr, 
                parallel, 
                lowMemory, 
                useStatistics, 
                glob, 
                allowMissingColumns, 
                rowIndexName, 
                rowIndexOffset, 
                includePathColumn,
                schemaPtr,        
                hiveSchemaPtr,    
                tryParseHiveDates
            );

            return ErrorHelper.Check(h);
        }
    }
    public static LazyFrameHandle ScanParquet(
        byte[] buffer,
        ulong? nRows,
        PlParallelStrategy parallel,
        bool lowMemory,
        bool useStatistics,
        bool glob,
        bool allowMissingColumns,
        string? rowIndexName,
        uint rowIndexOffset,
        string? includePathColumn,
        SchemaHandle? schema,              
        SchemaHandle? hivePartitionSchema, 
        bool tryParseHiveDates)
    {
        unsafe
        {
            fixed (byte* pBuf = buffer)
            {
                ulong nRowsVal = nRows.GetValueOrDefault();
                IntPtr nRowsPtr = nRows.HasValue ? (IntPtr)(&nRowsVal) : IntPtr.Zero;

                using var schemaLock = new SafeHandleLock<SchemaHandle>(
                    schema != null ? [schema] : null
                );
                IntPtr schemaPtr = schema != null ? schemaLock.Pointers[0] : IntPtr.Zero;

                using var hiveLock = new SafeHandleLock<SchemaHandle>(
                    hivePartitionSchema != null ? [hivePartitionSchema] : null
                );
                IntPtr hiveSchemaPtr = hivePartitionSchema != null ? hiveLock.Pointers[0] : IntPtr.Zero;

                var h = NativeBindings.pl_scan_parquet_memory(
                    (IntPtr)pBuf, (UIntPtr)buffer.Length,
                    nRowsPtr, 
                    parallel, 
                    lowMemory, 
                    useStatistics, 
                    glob, 
                    allowMissingColumns, 
                    rowIndexName, 
                    rowIndexOffset, 
                    includePathColumn,
                    schemaPtr,
                    hiveSchemaPtr,
                    tryParseHiveDates
                );

                return ErrorHelper.Check(h);
            }
        }
    }

    public static void WriteCsv(DataFrameHandle df, string path)
    {
        NativeBindings.pl_write_csv(df, path);
        ErrorHelper.CheckVoid();
    }
    public static void SinkCsv(LazyFrameHandle lf, string path)
    {
        NativeBindings.pl_lazy_sink_csv(lf, path);
        lf.TransferOwnership();
        ErrorHelper.CheckVoid();
    }
    public static void WriteParquet(
        DataFrameHandle df, 
        string path, 
        PlParquetCompression compression, 
        int compressionLevel,
        bool statistics,
        int rowGroupSize, 
        int dataPageSize,
        bool parallel)
    {
        nuint rgs = rowGroupSize > 0 ? (nuint)rowGroupSize : 0;
        nuint dps = dataPageSize > 0 ? (nuint)dataPageSize : 0;

        NativeBindings.pl_dataframe_write_parquet(
            df, 
            path, 
            compression, 
            compressionLevel, 
            statistics, 
            rgs, 
            dps, 
            parallel
        );
        
        ErrorHelper.CheckVoid();
    }
    public static void WriteIpc(
        DataFrameHandle df, 
        string path, 
        PlIpcCompression compression = PlIpcCompression.None, 
        bool parallel = true, 
        int compatLevel = -1)
    {
        NativeBindings.pl_dataframe_write_ipc(
            df, 
            path, 
            compression, 
            parallel, 
            compatLevel
        );
        
        ErrorHelper.CheckVoid();
    }

    public static void WriteJson(DataFrameHandle df, string path)
    {
        NativeBindings.pl_dataframe_write_json(df, path);
        ErrorHelper.CheckVoid();
    }
    // Sink Parquet
    public static void SinkParquet(
        LazyFrameHandle lf, 
        string path,
        PlParquetCompression compression,
        int compressionLevel,
        bool statistics,
        int rowGroupSize,
        int dataPageSize,
        bool maintainOrder,
        PlSyncOnClose syncOnClose,
        bool mkdir)
    {
        nuint rgs = rowGroupSize > 0 ? (nuint)rowGroupSize : 0;
        nuint dps = dataPageSize > 0 ? (nuint)dataPageSize : 0;

        NativeBindings.pl_lazyframe_sink_parquet(
            lf, 
            path, 
            compression, 
            compressionLevel, 
            statistics, 
            rgs, 
            dps, 
            maintainOrder, 
            syncOnClose, 
            mkdir
        );
        
        lf.TransferOwnership();
        ErrorHelper.CheckVoid();
    }
    // ---------------------------------------------------------
    // Read JSON (File)
    // ---------------------------------------------------------
    public static DataFrameHandle ReadJson(
        string path,
        string[]? columns,
        SchemaHandle? schema,
        ulong? inferSchemaLen, // Rust: Option<usize>
        ulong? batchSize,      // Rust: Option<usize>
        bool ignoreErrors,
        PlJsonFormat jsonFormat)
    {
        return UseUtf8StringArray(columns ?? [], colPtrs =>
        {
            unsafe
            {
                ulong inferVal = inferSchemaLen.GetValueOrDefault();
                IntPtr inferPtr = inferSchemaLen.HasValue ? (IntPtr)(&inferVal) : IntPtr.Zero;

                ulong batchVal = batchSize.GetValueOrDefault();
                IntPtr batchPtr = batchSize.HasValue ? (IntPtr)(&batchVal) : IntPtr.Zero;

                using var schemaLock = new SafeHandleLock<SchemaHandle>(
                    schema != null ? new[] { schema } : null
                );
                IntPtr schemaPtr = schema != null ? schemaLock.Pointers[0] : IntPtr.Zero;

                var h = NativeBindings.pl_read_json(
                    path,
                    colPtrs, (UIntPtr)(columns?.Length ?? 0),
                    schemaPtr,
                    inferPtr,
                    batchPtr,
                    ignoreErrors,
                    jsonFormat // Enum 转 byte
                );

                return ErrorHelper.Check(h);
            }
        });
    }

    // ---------------------------------------------------------
    // Read JSON (Memory / Bytes)
    // ---------------------------------------------------------
    public static DataFrameHandle ReadJson(
        byte[] buffer,
        string[]? columns,
        SchemaHandle? schema,
        ulong? inferSchemaLen,
        ulong? batchSize,
        bool ignoreErrors,
        PlJsonFormat jsonFormat)
    {
        return UseUtf8StringArray(columns ?? [], colPtrs =>
        {
            unsafe
            {
                fixed (byte* pBuf = buffer)
                {
                    ulong inferVal = inferSchemaLen.GetValueOrDefault();
                    IntPtr inferPtr = inferSchemaLen.HasValue ? (IntPtr)(&inferVal) : IntPtr.Zero;

                    ulong batchVal = batchSize.GetValueOrDefault();
                    IntPtr batchPtr = batchSize.HasValue ? (IntPtr)(&batchVal) : IntPtr.Zero;

                    using var schemaLock = new SafeHandleLock<SchemaHandle>(
                        schema != null ? new[] { schema } : null
                    );
                    IntPtr schemaPtr = schema != null ? schemaLock.Pointers[0] : IntPtr.Zero;

                    var h = NativeBindings.pl_read_json_memory(
                        (IntPtr)pBuf, (UIntPtr)buffer.Length,
                        colPtrs, (UIntPtr)(columns?.Length ?? 0),
                        schemaPtr,
                        inferPtr,
                        batchPtr,
                        ignoreErrors,
                        jsonFormat
                    );

                    return ErrorHelper.Check(h);
                }
            }
        });
    }

    // ---------------------------------------------------------
    // Scan NDJSON (File)
    // ---------------------------------------------------------
    public static LazyFrameHandle ScanNdjson(
        string path,
        SchemaHandle? schema,
        ulong? batchSize,
        ulong? inferSchemaLen,
        ulong? nRows,
        bool lowMemory,
        bool rechunk,
        bool ignoreErrors,
        string? rowIndexName,
        uint rowIndexOffset,
        string? includePathColumn)
    {
        unsafe
        {
            ulong batchVal = batchSize.GetValueOrDefault();
            IntPtr batchPtr = batchSize.HasValue ? (IntPtr)(&batchVal) : IntPtr.Zero;

            ulong inferVal = inferSchemaLen.GetValueOrDefault();
            IntPtr inferPtr = inferSchemaLen.HasValue ? (IntPtr)(&inferVal) : IntPtr.Zero;

            ulong nRowsVal = nRows.GetValueOrDefault();
            IntPtr nRowsPtr = nRows.HasValue ? (IntPtr)(&nRowsVal) : IntPtr.Zero;

            using var schemaLock = new SafeHandleLock<SchemaHandle>(
                schema != null ? new[] { schema } : null
            );
            IntPtr schemaPtr = schema != null ? schemaLock.Pointers[0] : IntPtr.Zero;

            // 3. 调用 Native
            var h = NativeBindings.pl_scan_ndjson(
                path,
                batchPtr,
                lowMemory,
                rechunk,
                schemaPtr,
                inferPtr,
                nRowsPtr,
                ignoreErrors,
                rowIndexName,
                rowIndexOffset,
                includePathColumn
            );

            return ErrorHelper.Check(h);
        }
    }

    // ---------------------------------------------------------
    // Scan NDJSON (Memory / Bytes)
    // ---------------------------------------------------------
    public static LazyFrameHandle ScanNdjson(
        byte[] buffer,
        SchemaHandle? schema,
        ulong? batchSize,
        ulong? inferSchemaLen,
        ulong? nRows,
        bool lowMemory,
        bool rechunk,
        bool ignoreErrors,
        string? rowIndexName,
        uint rowIndexOffset,
        string? includePathColumn)
    {
        unsafe
        {
            fixed (byte* pBuf = buffer)
            {
                ulong batchVal = batchSize.GetValueOrDefault();
                IntPtr batchPtr = batchSize.HasValue ? (IntPtr)(&batchVal) : IntPtr.Zero;

                ulong inferVal = inferSchemaLen.GetValueOrDefault();
                IntPtr inferPtr = inferSchemaLen.HasValue ? (IntPtr)(&inferVal) : IntPtr.Zero;

                ulong nRowsVal = nRows.GetValueOrDefault();
                IntPtr nRowsPtr = nRows.HasValue ? (IntPtr)(&nRowsVal) : IntPtr.Zero;

                using var schemaLock = new SafeHandleLock<SchemaHandle>(
                    schema != null ? new[] { schema } : null
                );
                IntPtr schemaPtr = schema != null ? schemaLock.Pointers[0] : IntPtr.Zero;
                
                var h = NativeBindings.pl_scan_ndjson_memory(
                    (IntPtr)pBuf, (UIntPtr)buffer.Length,
                    batchPtr,
                    lowMemory,
                    rechunk,
                    schemaPtr,
                    inferPtr,
                    nRowsPtr,
                    ignoreErrors,
                    rowIndexName,
                    rowIndexOffset,
                    includePathColumn
                );

                return ErrorHelper.Check(h);
            }
        }
    }
    public static void SinkJson(LazyFrameHandle lf, string path)
    {
        NativeBindings.pl_lazy_sink_json(lf, path);
        lf.TransferOwnership();
        ErrorHelper.CheckVoid();
    }
    // ---------------------------------------------------------
    // Read IPC (File)
    // ---------------------------------------------------------
    public static DataFrameHandle ReadIpc(
        string path,
        string[]? columns,
        ulong? nRows,
        string? rowIndexName,
        uint rowIndexOffset,
        bool rechunk,
        bool memoryMap,
        string? includePathColumn)
    {
        return UseUtf8StringArray(columns ?? System.Array.Empty<string>(), colPtrs =>
        {
            unsafe
            {
                ulong rowsVal = nRows.GetValueOrDefault();
                IntPtr rowsPtr = nRows.HasValue ? (IntPtr)(&rowsVal) : IntPtr.Zero;

                var h = NativeBindings.pl_read_ipc(
                    path,
                    colPtrs, 
                    (UIntPtr)(columns?.Length ?? 0),
                    rowsPtr,
                    rowIndexName,
                    rowIndexOffset,
                    rechunk,
                    memoryMap,
                    includePathColumn
                );

                return ErrorHelper.Check(h);
            }
        });
    }

    // ---------------------------------------------------------
    // Read IPC (Memory)
    // ---------------------------------------------------------
    public static DataFrameHandle ReadIpc(
        byte[] buffer,
        string[]? columns,
        ulong? nRows,
        string? rowIndexName,
        uint rowIndexOffset,
        bool rechunk,
        string? includePathColumn)
    {
        return UseUtf8StringArray(columns ?? System.Array.Empty<string>(), colPtrs =>
        {
            unsafe
            {
                fixed (byte* pBuf = buffer)
                {
                    ulong rowsVal = nRows.GetValueOrDefault();
                    IntPtr rowsPtr = nRows.HasValue ? (IntPtr)(&rowsVal) : IntPtr.Zero;

                    var h = NativeBindings.pl_read_ipc_memory(
                        (IntPtr)pBuf, 
                        (UIntPtr)buffer.Length,
                        colPtrs, 
                        (UIntPtr)(columns?.Length ?? 0),
                        rowsPtr,
                        rowIndexName,
                        rowIndexOffset,
                        rechunk,
                        includePathColumn
                    );

                    return ErrorHelper.Check(h);
                }
            }
        });
    }

    // ---------------------------------------------------------
    // Scan IPC (File)
    // ---------------------------------------------------------
    public static LazyFrameHandle ScanIpc(
        string path,
        SchemaHandle? schema,
        ulong? nRows,
        bool rechunk,
        bool cache,
        string? rowIndexName,
        uint rowIndexOffset,
        string? includePathColumn,
        bool hivePartitioning)
    {
        unsafe
        {
            ulong rowsVal = nRows.GetValueOrDefault();
            IntPtr rowsPtr = nRows.HasValue ? (IntPtr)(&rowsVal) : IntPtr.Zero;

            using var schemaLock = new SafeHandleLock<SchemaHandle>(
                schema != null ? new[] { schema } : null
            );
            IntPtr schemaPtr = schema != null ? schemaLock.Pointers[0] : IntPtr.Zero;

            // 3. 调用 Native
            var h = NativeBindings.pl_scan_ipc(
                path,
                schemaPtr,
                rowsPtr,
                rechunk,
                cache,
                rowIndexName,
                rowIndexOffset,
                includePathColumn,
                hivePartitioning
            );

            return ErrorHelper.Check(h);
        }
    }

    // ---------------------------------------------------------
    // Scan IPC (Memory / Bytes)
    // ---------------------------------------------------------
    public static LazyFrameHandle ScanIpc(
        byte[] buffer,
        SchemaHandle? schema,
        ulong? nRows,
        bool rechunk,
        bool cache,
        string? rowIndexName,
        uint rowIndexOffset,
        bool hivePartitioning)
    {
        unsafe
        {
            fixed (byte* pBuf = buffer)
            {
                ulong rowsVal = nRows.GetValueOrDefault();
                IntPtr rowsPtr = nRows.HasValue ? (IntPtr)(&rowsVal) : IntPtr.Zero;

                using var schemaLock = new SafeHandleLock<SchemaHandle>(
                    schema != null ? new[] { schema } : null
                );
                IntPtr schemaPtr = schema != null ? schemaLock.Pointers[0] : IntPtr.Zero;

                var h = NativeBindings.pl_scan_ipc_memory(
                    (IntPtr)pBuf, (UIntPtr)buffer.Length,
                    schemaPtr,
                    rowsPtr,
                    rechunk,
                    cache,
                    rowIndexName,
                    rowIndexOffset,
                    null, 
                    hivePartitioning
                );

                return ErrorHelper.Check(h);
            }
        }
    }

    /// <summary>
    /// Sinks the LazyFrame to an IPC file. 
    /// Consumes the LazyFrame handle.
    /// </summary>
    public static void SinkIpc(
        LazyFrameHandle lf, 
        string path,
        PlIpcCompression compression,
        int compatLevel,
        bool maintainOrder,
        PlSyncOnClose syncOnClose,
        bool mkdir)
    {
        NativeBindings.pl_lazyframe_sink_ipc(
            lf, 
            path, 
            compression, 
            compatLevel, 
            maintainOrder, 
            syncOnClose, 
            mkdir
        );
        
        lf.TransferOwnership();
        
        ErrorHelper.CheckVoid();
    }
    public static unsafe DataFrameHandle FromArrow(RecordBatch batch)
    {
        // Alloc C struct at stack
        var cArray = new CArrowArray();
        var cSchema = new CArrowSchema();

        // Export to C Arrow
        // Step A: Export Data 
        CArrowArrayExporter.ExportRecordBatch(batch, &cArray);

        // Step B: Export Schema 
        CArrowSchemaExporter.ExportSchema(batch.Schema, &cSchema);

        // Transfer to Rust
        var h = NativeBindings.pl_dataframe_from_arrow_record_batch(&cArray, &cSchema);
        
        return ErrorHelper.Check(h);
    }
    public static unsafe LazyFrameHandle LazyFrameScanStream(
        CArrowSchema* schema,
        delegate* unmanaged[Cdecl]<void*, Arrow.CArrowArrayStream*> callback,
        delegate* unmanaged[Cdecl]<void*, void> destroyCallback,
        void* userData)
    {
        var handle = NativeBindings.pl_lazy_frame_scan_stream(schema, callback,destroyCallback, userData);
        return ErrorHelper.Check(handle);
    }
    public static void ExportBatches(DataFrameHandle dfHandle, Action<RecordBatch> onBatchReceived)
    {
        var (callback, cleanup, userData) = ArrowStreamInterop.PrepareSink(onBatchReceived);

        NativeBindings.pl_dataframe_export_batches(
            dfHandle, 
            callback, 
            cleanup, 
            userData
        );
        ErrorHelper.CheckVoid();
    }

    public static LazyFrameHandle SinkBatches(LazyFrameHandle lf, Action<Apache.Arrow.RecordBatch> onBatchReceived)
    {
        // Prepare Interop Resource (Delegate, GCHandle, Cleanup)
        var (callback, cleanup, userData) = ArrowStreamInterop.PrepareSink(onBatchReceived);

        var handle = NativeBindings.pl_lazy_map_batches(
            lf,
            callback,
            cleanup,
            userData
        );
        lf.TransferOwnership();
        return ErrorHelper.Check(handle);
    }
    // ---------------------------------------------------------
    // Read Excel
    // ---------------------------------------------------------
    public static DataFrameHandle ReadExcel(
        string path,
        string? sheetName,
        ulong sheetIndex,
        SchemaHandle? schema,
        bool hasHeader,
        ulong inferSchemaLen,
        bool dropEmptyRows,
        bool raiseIfEmpty)
    {
        unsafe
        {
            using var schemaLock = new SafeHandleLock<SchemaHandle>(
                schema != null ? new[] { schema } : null
            );
            IntPtr schemaPtr = schema != null ? schemaLock.Pointers[0] : IntPtr.Zero;

            var h = NativeBindings.pl_read_excel(
                path,
                sheetName,
                (UIntPtr)sheetIndex, // ulong -> usize
                schemaPtr,
                hasHeader,
                (UIntPtr)inferSchemaLen,
                dropEmptyRows,
                raiseIfEmpty
            );

            return ErrorHelper.Check(h);
        }
    }

    // ---------------------------------------------------------
    // Write Excel
    // ---------------------------------------------------------
    public static void WriteExcel(
        DataFrameHandle handle,
        string path,
        string? sheetName,
        string? dateFormat,
        string? datetimeFormat)
    {
        if (handle.IsInvalid)
            throw new ObjectDisposedException(nameof(handle), "DataFrame handle is invalid or closed.");
        NativeBindings.pl_write_excel(handle, path, sheetName, dateFormat, datetimeFormat);
    }
}