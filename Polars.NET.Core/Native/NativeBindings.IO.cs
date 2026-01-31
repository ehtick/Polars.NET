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
        // [Removed] bool useStatistics
    );

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_write_parquet(DataFrameHandle df, string path);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_lazy_sink_parquet(
        LazyFrameHandle lf, 
        [MarshalAs(UnmanagedType.LPUTF8Str)] string path
    );

    // JSON
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)] 
    public static partial DataFrameHandle pl_read_json(string path);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)] 
    public static partial LazyFrameHandle pl_scan_ndjson(string path);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_dataframe_write_json(DataFrameHandle df, string path);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)] 
    public static partial void pl_lazy_sink_json(LazyFrameHandle lf, string path);

    // IPC
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)] 
    public static partial DataFrameHandle pl_read_ipc(string path);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)] 
    public static partial LazyFrameHandle pl_scan_ipc(string path);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_dataframe_write_ipc(DataFrameHandle df, string path);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)] 
    public static partial void pl_lazy_sink_ipc(LazyFrameHandle lf, string path);

}