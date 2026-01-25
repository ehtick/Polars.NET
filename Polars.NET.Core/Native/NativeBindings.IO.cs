using System.Runtime.InteropServices;

namespace Polars.NET.Core.Native;

unsafe internal partial class NativeBindings
{
    const string LibName = "native_shim";
    // CSV
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_read_csv(
    string path,
    SchemaHandle schema,
    [MarshalAs(UnmanagedType.I1)] bool hasHeader,
    byte separator,
    UIntPtr skipRows,
    [MarshalAs(UnmanagedType.I1)] bool tryParseDates 
    );

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_scan_csv(
        string path,
        SchemaHandle schema,
        [MarshalAs(UnmanagedType.I1)] bool hasHeader,
        byte separator,
        UIntPtr skipRows,
        [MarshalAs(UnmanagedType.I1)] bool tryParseDates
    );

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_write_csv(DataFrameHandle df, string path);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)] 
    public static partial void pl_lazy_sink_csv(LazyFrameHandle lf, string path);

    // Parquet
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)] 
    public static partial LazyFrameHandle pl_scan_parquet(string path);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_read_parquet(string path);

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