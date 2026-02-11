using System.Runtime.InteropServices;

namespace Polars.NET.Core.Native;

unsafe internal partial class NativeBindings
{
    const string LibName = "native_shim";
    // CSV
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_read_csv(
        string path,

        // --- 1. Columns ---
        [MarshalAs(UnmanagedType.LPArray, ArraySubType = UnmanagedType.LPStr)] string[]? col_names,
        nuint col_names_len,

        // --- 2. Core Configs ---
        [MarshalAs(UnmanagedType.U1)] bool has_header,
        byte separator,
        byte quote_char,
        byte eol_char,
        [MarshalAs(UnmanagedType.U1)] bool ignore_errors,
        [MarshalAs(UnmanagedType.U1)] bool try_parse_dates,
        [MarshalAs(UnmanagedType.U1)] bool low_memory,

        // --- 3. Optional Sizes (Pointers for Option<usize>) ---
        nuint skip_rows,
        IntPtr n_rows_ptr,          // nuint*
        IntPtr infer_schema_len_ptr,// nuint*

        // --- 4. Schema & Encoding ---
        SchemaHandle schema,        // Nullable handle
        PlCsvEncoding encoding,

        // --- 5. Advanced Parsing ---
        [MarshalAs(UnmanagedType.LPArray, ArraySubType = UnmanagedType.LPStr)] string[]? null_values,
        nuint null_values_len,
        [MarshalAs(UnmanagedType.U1)] bool missing_is_null,
        string? comment_prefix,
        [MarshalAs(UnmanagedType.U1)] bool decimal_comma,
        [MarshalAs(UnmanagedType.U1)] bool truncate_ragged_lines,

        // --- 6. Row Index ---
        string? row_index_name,
        nuint row_index_offset
    );

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_scan_csv(
        string path,
        
        // --- Core ---
        [MarshalAs(UnmanagedType.U1)] bool has_header,
        byte separator,
        byte quote_char,       
        byte eol_char,         
        [MarshalAs(UnmanagedType.U1)] bool ignore_errors,
        [MarshalAs(UnmanagedType.U1)] bool try_parse_dates,
        [MarshalAs(UnmanagedType.U1)] bool low_memory,
        [MarshalAs(UnmanagedType.U1)] bool cache,
        [MarshalAs(UnmanagedType.U1)] bool rechunk,

        // --- Sizes ---
        nuint skip_rows,
        IntPtr n_rows_ptr,
        IntPtr infer_schema_len_ptr,

        // --- Row Index ---
        string? row_index_name,
        nuint row_index_offset,

        // --- Schema ---
        SchemaHandle schema,
        PlCsvEncoding encoding,

        // --- Advanced ---
        [MarshalAs(UnmanagedType.LPArray, ArraySubType = UnmanagedType.LPStr)] string[]? null_values, 
        nuint null_values_len,
        [MarshalAs(UnmanagedType.U1)] bool missing_is_null,
        string? comment_prefix,
        [MarshalAs(UnmanagedType.U1)] bool decimal_comma,
        nuint chunkSize
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_scan_csv_mem(
        byte* bufferPtr,        // Buffer Pointer
        UIntPtr bufferLen,      // Buffer Length
        // --- Core ---
        [MarshalAs(UnmanagedType.U1)] bool has_header,
        byte separator,
        byte quote_char,       
        byte eol_char,         
        [MarshalAs(UnmanagedType.U1)] bool ignore_errors,
        [MarshalAs(UnmanagedType.U1)] bool try_parse_dates,
        [MarshalAs(UnmanagedType.U1)] bool low_memory,
        [MarshalAs(UnmanagedType.U1)] bool cache,
        [MarshalAs(UnmanagedType.U1)] bool rechunk,

        // --- Sizes ---
        nuint skip_rows,
        IntPtr n_rows_ptr,
        IntPtr infer_schema_len_ptr,

        // --- Row Index ---
        string? row_index_name,
        nuint row_index_offset,

        // --- Schema ---
        SchemaHandle schema,
        PlCsvEncoding encoding,

        // --- Advanced ---
        [MarshalAs(UnmanagedType.LPArray, ArraySubType = UnmanagedType.LPStr)] string[]? null_values, 
        nuint null_values_len,
        [MarshalAs(UnmanagedType.U1)] bool missing_is_null,
        string? comment_prefix,
        [MarshalAs(UnmanagedType.U1)] bool decimal_comma
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_dataframe_write_csv(
        DataFrameHandle df, 
        string path,
        
        // CsvWriter Specific
        [MarshalAs(UnmanagedType.U1)] bool has_header,
        [MarshalAs(UnmanagedType.U1)] bool use_bom,
        nuint batch_size,

        // SerializeOptions
        byte separator,
        byte quote_char,
        PlQuoteStyle quote_style,
        string? null_value,        
        string? line_terminator,
        string? date_format,
        string? time_format,
        string? datetime_format,
        int float_scientific,       // -1: null, 0: false, 1: true
        int float_precision,        // -1: null, >=0: precision
        [MarshalAs(UnmanagedType.U1)] bool decimal_comma
    );

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_lazyframe_sink_csv(
        LazyFrameHandle lf, 
        string path,
        
        // CsvWriterOptions
        [MarshalAs(UnmanagedType.U1)] bool include_header,
        [MarshalAs(UnmanagedType.U1)] bool include_bom,
        nuint batch_size,
        
        // SinkOptions
        [MarshalAs(UnmanagedType.U1)] bool maintain_order,
        PlSyncOnClose sync_on_close,
        [MarshalAs(UnmanagedType.U1)] bool mkdir,

        // SerializeOptions
        byte separator,
        byte quote_char,
        PlQuoteStyle quote_style,
        string? null_value,
        string? line_terminator,
        string? date_format,
        string? time_format,
        string? datetime_format,
        int float_scientific,
        int float_precision,
        [MarshalAs(UnmanagedType.U1)] bool decimal_comma
    );

    // Parquet
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_scan_parquet(
        string path,
        IntPtr n_rows, // null for None
        PlParallelStrategy parallel_code,
        [MarshalAs(UnmanagedType.I1)] bool low_memory,
        [MarshalAs(UnmanagedType.I1)] bool use_statistics,
        [MarshalAs(UnmanagedType.I1)] bool glob,
        [MarshalAs(UnmanagedType.I1)] bool allow_missing_columns,
        [MarshalAs(UnmanagedType.I1)] bool rechunk, 
        [MarshalAs(UnmanagedType.I1)] bool cache,   
        string? row_index_name,
        uint row_index_offset,
        string? include_path_col,
        IntPtr schema,
        IntPtr hive_schema,
        [MarshalAs(UnmanagedType.I1)] bool try_parse_hive_dates,
        // Cloud Options
        PlCloudProvider cloud_provider,
        UIntPtr cloud_retries,
        ulong cloud_cache_ttl,
        [MarshalAs(UnmanagedType.LPArray, ArraySubType = UnmanagedType.LPUTF8Str)] string[]? cloud_keys,
        [MarshalAs(UnmanagedType.LPArray, ArraySubType = UnmanagedType.LPUTF8Str)] string[]? cloud_values,
        UIntPtr cloud_len
    );
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_scan_parquet_memory(
        IntPtr buffer_ptr,
        UIntPtr buffer_len,
        IntPtr n_rows,
        PlParallelStrategy parallel_code,
        [MarshalAs(UnmanagedType.I1)] bool low_memory,
        [MarshalAs(UnmanagedType.I1)] bool use_statistics,
        [MarshalAs(UnmanagedType.I1)] bool glob,
        [MarshalAs(UnmanagedType.I1)] bool allow_missing_columns,
        [MarshalAs(UnmanagedType.I1)] bool rechunk, 
        [MarshalAs(UnmanagedType.I1)] bool cache,   
        string? row_index_name,
        uint row_index_offset,
        string? include_path_col,
        IntPtr schema,
        IntPtr hive_schema,
        [MarshalAs(UnmanagedType.I1)] bool try_parse_hive_dates
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_read_parquet(
        string path,
        IntPtr[] columns, UIntPtr columnsLen,
        IntPtr limit, 
        PlParallelStrategy parallel,
        [MarshalAs(UnmanagedType.U1)] bool lowMemory,
        string? rowIndexName,
        uint rowIndexOffset
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_read_parquet_memory(
        IntPtr buffer, UIntPtr bufferLen,
        IntPtr[] columns, UIntPtr columnsLen,
        IntPtr limit,
        PlParallelStrategy parallel,
        [MarshalAs(UnmanagedType.U1)] bool lowMemory,
        string? rowIndexName,
        uint rowIndexOffset
    );

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_dataframe_write_parquet(
        DataFrameHandle df, 
        string path,
        PlParquetCompression compression,
        int compression_level,      // -1 default
        [MarshalAs(UnmanagedType.U1)] bool statistics,
        nuint row_group_size,       // usize -> nuint, 0 default
        nuint data_page_size,       // usize -> nuint, 0 default
        [MarshalAs(UnmanagedType.U1)] bool parallel
    );

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_lazyframe_sink_parquet(
        LazyFrameHandle lf, 
        string path,
        PlParquetCompression compression,
        int compression_level,
        [MarshalAs(UnmanagedType.U1)] bool statistics,
        nuint row_group_size,
        nuint data_page_size,
        [MarshalAs(UnmanagedType.U1)] bool maintain_order,
        PlSyncOnClose sync_on_close,
        [MarshalAs(UnmanagedType.U1)] bool mkdir,
        PlCloudProvider cloud_provider,
        UIntPtr cloud_retries,
        ulong cloud_cache_ttl,
        [MarshalAs(UnmanagedType.LPArray, ArraySubType = UnmanagedType.LPUTF8Str)] string[]? cloud_keys,
        [MarshalAs(UnmanagedType.LPArray, ArraySubType = UnmanagedType.LPUTF8Str)] string[]? cloud_values,
        UIntPtr cloud_len
    );

    // ---------------------------------------------------------
    // Read JSON (File)
    // ---------------------------------------------------------
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_read_json(
        string path,
        // columns (Option<Vec<String>>)
        IntPtr[] columns, UIntPtr columnsLen,
        // schema (Option<Schema>)
        IntPtr schema, 
        // infer_schema_len (Option<usize>) -> pointer to usize
        IntPtr inferSchemaLen, 
        // batch_size (Option<usize>) -> pointer to usize
        IntPtr batchSize,
        // ignore_errors (bool)
        [MarshalAs(UnmanagedType.U1)] bool ignoreErrors,
        // format (u8 enum)
        PlJsonFormat jsonFormat
    );

    // ---------------------------------------------------------
    // Read JSON (Memory)
    // ---------------------------------------------------------
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_read_json_memory(
        IntPtr buffer, UIntPtr bufferLen,
        IntPtr[] columns, UIntPtr columnsLen,
        IntPtr schema,
        IntPtr inferSchemaLen,
        IntPtr batchSize,
        [MarshalAs(UnmanagedType.U1)] bool ignoreErrors,
        PlJsonFormat jsonFormat
    );
    // ---------------------------------------------------------
    // Scan NDJSON (File)
    // ---------------------------------------------------------
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_scan_ndjson(
        string path,
        // Options
        IntPtr batchSize,           // *const usize
        [MarshalAs(UnmanagedType.U1)] bool lowMemory,
        [MarshalAs(UnmanagedType.U1)] bool rechunk,
        IntPtr schema,              // *mut SchemaContext (Overwrite)
        IntPtr inferSchemaLen,      // *const usize
        IntPtr nRows,               // *const usize
        [MarshalAs(UnmanagedType.U1)] bool ignoreErrors,
        string? rowIndexName,
        uint rowIndexOffset,
        string? includePathColumn
    );

    // ---------------------------------------------------------
    // Scan NDJSON (Memory)
    // ---------------------------------------------------------
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_scan_ndjson_memory(
        IntPtr buffer, UIntPtr bufferLen,
        // Options (Same as above)
        IntPtr batchSize,
        [MarshalAs(UnmanagedType.U1)] bool lowMemory,
        [MarshalAs(UnmanagedType.U1)] bool rechunk,
        IntPtr schema,
        IntPtr inferSchemaLen,
        IntPtr nRows,
        [MarshalAs(UnmanagedType.U1)] bool ignoreErrors,
        string? rowIndexName,
        uint rowIndexOffset,
        string? includePathColumn
    );
    // -----------------------------------------------------
    // Write & Sink JSON
    // -----------------------------------------------------
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_dataframe_write_json(
        DataFrameHandle df, 
        string path,
        PlJsonFormat jsonFormat
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_lazyframe_sink_json(
        LazyFrameHandle lf, 
        string path,
        PlJsonFormat json_format,
        [MarshalAs(UnmanagedType.U1)] bool maintain_order,
        PlSyncOnClose sync_on_close,
        [MarshalAs(UnmanagedType.U1)] bool mkdir
    );

    // IPC
    // ---------------------------------------------------------
    // Read IPC (File)
    // ---------------------------------------------------------
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_read_ipc(
        string path,
        // Options
        IntPtr[] columns, 
        UIntPtr columnsLen,
        IntPtr nRows,               // *const usize
        string? rowIndexName,
        uint rowIndexOffset,
        [MarshalAs(UnmanagedType.U1)] bool rechunk,
        [MarshalAs(UnmanagedType.U1)] bool memoryMap,
        string? includePathColumn
    );

    // ---------------------------------------------------------
    // Read IPC (Memory)
    // ---------------------------------------------------------
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_read_ipc_memory(
        IntPtr buffer, 
        UIntPtr bufferLen,
        // Options (Same as above)
        IntPtr[] columns, 
        UIntPtr columnsLen,
        IntPtr nRows,
        string? rowIndexName,
        uint rowIndexOffset,
        [MarshalAs(UnmanagedType.U1)] bool rechunk,
        string? includePathColumn
    );
    // ---------------------------------------------------------
    // Scan IPC (File)
    // ---------------------------------------------------------
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_scan_ipc(
        string path,
        // Unified Args
        IntPtr schema,              // *mut SchemaContext
        IntPtr nRows,               // *const usize (PreSlice)
        [MarshalAs(UnmanagedType.U1)] bool rechunk,
        [MarshalAs(UnmanagedType.U1)] bool cache,
        string? rowIndexName,
        uint rowIndexOffset,
        string? includePathColumn,
        [MarshalAs(UnmanagedType.U1)] bool hivePartitioning
    );

    // ---------------------------------------------------------
    // Scan IPC (Memory)
    // ---------------------------------------------------------
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_scan_ipc_memory(
        IntPtr buffer, UIntPtr bufferLen,
        // Unified Args
        IntPtr schema,
        IntPtr nRows,
        [MarshalAs(UnmanagedType.U1)] bool rechunk,
        [MarshalAs(UnmanagedType.U1)] bool cache,
        string? rowIndexName,
        uint rowIndexOffset,
        string? includePathColumn,
        [MarshalAs(UnmanagedType.U1)] bool hivePartitioning
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_dataframe_write_ipc(
        DataFrameHandle df, 
        string path, 
        PlIpcCompression compression, 
        [MarshalAs(UnmanagedType.U1)] bool parallel, 
        int compat_level
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_lazyframe_sink_ipc(
        LazyFrameHandle lf, 
        string path,
        PlIpcCompression compression,
        int compat_level,
        [MarshalAs(UnmanagedType.U1)] bool maintain_order,
        PlSyncOnClose sync_on_close,
        [MarshalAs(UnmanagedType.U1)] bool mkdir
    );

    // ---------------------------------------------------------
    // Read Excel (Calamine Engine)
    // ---------------------------------------------------------
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_read_excel(
        string path,
        string? sheetName,
        UIntPtr sheetIndex,
        IntPtr schema,               // *mut SchemaContext
        [MarshalAs(UnmanagedType.U1)] bool hasHeader,
        UIntPtr inferSchemaLen,      // usize
        [MarshalAs(UnmanagedType.U1)] bool dropEmptyRows,
        [MarshalAs(UnmanagedType.U1)] bool raiseIfEmpty
    );

    // ---------------------------------------------------------
    // Write Excel (Native Rust Engine)
    // ---------------------------------------------------------
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_write_excel(
        DataFrameHandle df,
        string path,
        string? sheetName,       // Option<&str>
        string? dateFormat,      // Option<&str>
        string? datetimeFormat   // Option<&str>
    );

    // ==========================================
    // Delta Lake
    // ==========================================
    
    /// <summary>
    /// Rust Signature:
    // pub extern "C" fn pl_scan_delta(
    //     path_ptr: *const c_char,
    //     // --- Time Travel Args ---
    //     version: *const i64,
    //     datetime_ptr: *const c_char,
    //     // --- Scan Args ---
    //     n_rows: *const usize,
    //     parallel_code: u8,
    //     low_memory: bool,
    //     use_statistics: bool,
    //     glob: bool,
    //     allow_missing_columns: bool, // <--- 这一点至关重要，必须为 true
    //     rechunk: bool, 
    //     cache: bool,   
    //     // --- Optional Names ---
    //     row_index_name_ptr: *const c_char,
    //     row_index_offset: u32,
    //     include_path_col_ptr: *const c_char,
    //     // --- Schema ---
    //     schema_ptr: *mut SchemaContext, // 用户手动传的 Schema (优先级最高)
    //     hive_schema_ptr: *mut SchemaContext,
    //     try_parse_hive_dates: bool,
    //     // --- Cloud Options ---
    //     cloud_provider: u8,
    //     cloud_retries: usize,
    //     cloud_cache_ttl: u64,
    //     cloud_keys: *const *const c_char,
    //     cloud_values: *const *const c_char,
    //     cloud_len: usize
    // ) -> *mut LazyFrameContext {
    /// </summary>
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_scan_delta(
        string path,
        // --- Time Travel ---
        IntPtr version,
        string? datetime,
        // --- Scan Args ---
        IntPtr n_rows, // null for None
        PlParallelStrategy parallel_code,
        [MarshalAs(UnmanagedType.I1)] bool low_memory,
        [MarshalAs(UnmanagedType.I1)] bool use_statistics,
        [MarshalAs(UnmanagedType.I1)] bool glob,
        // [MarshalAs(UnmanagedType.I1)] bool allow_missing_columns,
        [MarshalAs(UnmanagedType.I1)] bool rechunk, 
        [MarshalAs(UnmanagedType.I1)] bool cache,   
        // --- Option Names ---
        string? row_index_name,
        uint row_index_offset,
        string? include_path_col,
        // --- Schema ---
        IntPtr schema,
        IntPtr hive_schema,
        [MarshalAs(UnmanagedType.I1)] bool try_parse_hive_dates,
        // --- Cloud Options (Must match pl_scan_parquet pattern) ---
        PlCloudProvider cloud_provider,
        UIntPtr cloud_retries,
        ulong cloud_cache_ttl,
        [MarshalAs(UnmanagedType.LPArray)] string[]? cloud_keys,
        [MarshalAs(UnmanagedType.LPArray)] string[]? cloud_values,
        UIntPtr cloud_len
    );
    // [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    // public static partial void pl_sink_delta(
    //     LazyFrameHandle lf,
    //     string path,
    //     PlDeltaSaveMode mode,
    //     // ... Cloud Options ...
    //     PlCloudProvider cloud_provider,
    //     UIntPtr cloud_retries,
    //     ulong cloud_cache_ttl,
    //     [MarshalAs(UnmanagedType.LPArray)] string[]? cloud_keys,
    //     [MarshalAs(UnmanagedType.LPArray)] string[]? cloud_values,
    //     UIntPtr cloud_len
    // );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_sink_delta(
        LazyFrameHandle lf, 
        string path, 
        PlDeltaSaveMode mode,
        // Parquet Options
        PlParquetCompression compression, 
        int compression_level, 
        [MarshalAs(UnmanagedType.U1)] bool statistics, 
        nuint row_group_size, 
        nuint data_page_size,
        // Sink Options
        [MarshalAs(UnmanagedType.U1)] bool maintain_order,
        // Cloud Options
        PlCloudProvider cloud_provider, 
        UIntPtr cloud_retries, 
        ulong cloud_cache_ttl, 
        [MarshalAs(UnmanagedType.LPArray)] string[]? cloud_keys, 
        [MarshalAs(UnmanagedType.LPArray)] string[]? cloud_values, 
        nuint cloud_len
    );
}