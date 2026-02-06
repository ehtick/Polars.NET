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
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_scan_parquet(
        string path,
        IntPtr nRows,               // *const usize
        PlParallelStrategy parallel,
        [MarshalAs(UnmanagedType.U1)] bool lowMemory,
        [MarshalAs(UnmanagedType.U1)] bool useStatistics,
        [MarshalAs(UnmanagedType.U1)] bool glob,
        [MarshalAs(UnmanagedType.U1)] bool allowMissingColumns,
        string? rowIndexName,
        uint rowIndexOffset,
        string? includePathColumn,
        IntPtr schema,              // *mut SchemaContext (Override Schema)
        IntPtr hiveSchema,          // *mut SchemaContext (Hive Partition Schema)
        [MarshalAs(UnmanagedType.U1)] bool tryParseHiveDates
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_scan_parquet_memory(
        IntPtr buffer, UIntPtr bufferLen,
        IntPtr nRows,               // *const usize
        PlParallelStrategy parallel,
        [MarshalAs(UnmanagedType.U1)] bool lowMemory,
        [MarshalAs(UnmanagedType.U1)] bool useStatistics,
        [MarshalAs(UnmanagedType.U1)] bool glob,
        [MarshalAs(UnmanagedType.U1)] bool allowMissingColumns,
        string? rowIndexName,
        uint rowIndexOffset,
        string? includePathColumn,
        IntPtr schema,              // *mut SchemaContext
        IntPtr hiveSchema,          // *mut SchemaContext
        [MarshalAs(UnmanagedType.U1)] bool tryParseHiveDates
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
        [MarshalAs(UnmanagedType.U1)] bool mkdir
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

}