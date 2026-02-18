using System.Runtime.InteropServices;

namespace Polars.NET.Core.Native;

unsafe internal partial class NativeBindings
{
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_io_delta_vacuum(
        string path,
        int retentionHours,
        [MarshalAs(UnmanagedType.U1)] bool enforceRetention,
        [MarshalAs(UnmanagedType.U1)] bool dryRun,
        [MarshalAs(UnmanagedType.U1)] bool vacuumModeFull,
        // Cloud Args (Simplified)
        [MarshalAs(UnmanagedType.LPArray)] string[]? keys,
        [MarshalAs(UnmanagedType.LPArray)] string[]? values,
        nuint len,
        out nuint filesDeleted
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_io_delta_restore(
        string path,
        long targetVersion,
        long targetTimestampMs,
        [MarshalAs(UnmanagedType.U1)] bool ignoreMissingFiles,
        [MarshalAs(UnmanagedType.U1)] bool protocolDowngradeAllowed,
        
        // Cloud Options
        [MarshalAs(UnmanagedType.LPArray)] string[]? keys,
        [MarshalAs(UnmanagedType.LPArray)] string[]? values,
        nuint len,
        
        // Output
        out long newVersion
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_io_delta_history(
        string path,
        nuint limit,
        // Cloud Options
        [MarshalAs(UnmanagedType.LPArray)] string[]? keys,
        [MarshalAs(UnmanagedType.LPArray)] string[]? values,
        nuint len,
        // Output: 指向 JSON 字符串的指针
        out IntPtr jsonPtr
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_io_delta_optimize(
        string path,
        long target_size_mb,
        string? filter_json,
        // Z-Order
        [MarshalAs(UnmanagedType.LPArray)] string[]? z_order_cols,
        nuint z_order_len,
        // Cloud Options
        PlCloudProvider cloud_provider,
        UIntPtr cloud_retries,
        ulong cloud_retry_timeout_ms,
        ulong cloud_retry_init_backoff_ms,
        ulong cloud_retry_max_backoff_ms,
        ulong cloud_cache_ttl,
        [MarshalAs(UnmanagedType.LPArray)] string[]? keys,
        [MarshalAs(UnmanagedType.LPArray)] string[]? values,
        nuint cloud_len,

        out nuint optimized_files
    );
}