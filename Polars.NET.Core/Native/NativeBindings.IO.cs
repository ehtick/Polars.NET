using System.Runtime.InteropServices;

namespace Polars.NET.Core.Native;

unsafe internal partial class NativeBindings
{
    const string LibName = "native_shim";
    // CSV
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_read_csv(
        string path,

        // ---  Columns Projection ---
        [In] IntPtr[] colNames, 
        UIntPtr colNamesLen,

        // --- Core Configs ---
        [MarshalAs(UnmanagedType.I1)] bool hasHeader,
        byte separator,
        [MarshalAs(UnmanagedType.I1)] bool ignoreErrors,     
        [MarshalAs(UnmanagedType.I1)] bool tryParseDates,
        [MarshalAs(UnmanagedType.I1)] bool lowMemory,        

        // --- Sizes ---
        UIntPtr skipRows,
        UIntPtr* nRowsPtr,           
        UIntPtr* inferSchemaLenPtr,  

        // --- Schema & Encoding ---
        SchemaHandle schema,         
        PlEncoding encoding         
    );

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_scan_csv(
        string path,

        // Core Configs
        [MarshalAs(UnmanagedType.I1)] bool hasHeader,
        byte separator,
        [MarshalAs(UnmanagedType.I1)] bool ignoreErrors,    // New
        [MarshalAs(UnmanagedType.I1)] bool tryParseDates,
        [MarshalAs(UnmanagedType.I1)] bool lowMemory,       // New
        [MarshalAs(UnmanagedType.I1)] bool cache,           // New
        [MarshalAs(UnmanagedType.I1)] bool rechunk,         // New

        // Sizes
        UIntPtr skipRows,
        UIntPtr* nRowsPtr,           // New (Optional)
        UIntPtr* inferSchemaLenPtr,  // New (Optional)

        // Row Index (New)
        string? rowIndexName,
        UIntPtr rowIndexOffset,

        // Schema & Encoding
        SchemaHandle schema,
        PlEncoding encoding          // New
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
        public static partial LazyFrameHandle pl_scan_csv_mem(
            byte* bufferPtr,        // Buffer Pointer
            UIntPtr bufferLen,      // Buffer Length

            [MarshalAs(UnmanagedType.I1)] bool hasHeader,
            byte separator,
            [MarshalAs(UnmanagedType.I1)] bool ignoreErrors,
            [MarshalAs(UnmanagedType.I1)] bool tryParseDates,
            [MarshalAs(UnmanagedType.I1)] bool lowMemory,
            [MarshalAs(UnmanagedType.I1)] bool cache,
            [MarshalAs(UnmanagedType.I1)] bool rechunk,

            UIntPtr skipRows,
            UIntPtr* nRowsPtr,
            UIntPtr* inferSchemaLenPtr,

            string? rowIndexName,
            UIntPtr rowIndexOffset,

            SchemaHandle schema,
            PlEncoding encoding
        );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_write_csv(DataFrameHandle df, string path);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)] 
    public static partial void pl_lazy_sink_csv(LazyFrameHandle lf, string path);

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
    public static partial void pl_write_parquet(DataFrameHandle df, string path);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_lazy_sink_parquet(
        LazyFrameHandle lf, 
        [MarshalAs(UnmanagedType.LPUTF8Str)] string path
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
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_dataframe_write_json(DataFrameHandle df, string path);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)] 
    public static partial void pl_lazy_sink_json(LazyFrameHandle lf, string path);

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
    public static partial void pl_dataframe_write_ipc(DataFrameHandle df, string path);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)] 
    public static partial void pl_lazy_sink_ipc(LazyFrameHandle lf, string path);

}