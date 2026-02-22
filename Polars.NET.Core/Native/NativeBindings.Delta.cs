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
        string[]? keys,
        string[]? values,
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
        string[]? keys,
        string[]? values,
        nuint len,
        
        // Output
        out long newVersion
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_io_delta_history(
        string path,
        nuint limit,
        // Cloud Options
        string[]? keys,
        string[]? values,
        nuint len,
        // Output: ptr to json
        out IntPtr jsonPtr
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_io_delta_optimize(
        string path,
        long target_size_mb,
        string? filter_json,
        // Z-Order
        string[]? z_order_cols,
        nuint z_order_len,
        // Cloud Options
        PlCloudProvider cloud_provider,
        UIntPtr cloud_retries,
        ulong cloud_retry_timeout_ms,
        ulong cloud_retry_init_backoff_ms,
        ulong cloud_retry_max_backoff_ms,
        ulong cloud_cache_ttl,
        string[]? keys,
        string[]? values,
        nuint cloud_len,

        out nuint optimized_files
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_io_delta_add_feature(
        string path,
        string feature_name,
        [MarshalAs(UnmanagedType.U1)] bool allowProtocolIncrease,
        string[]? keys,
        string[]? values,
        nuint cloud_len
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_io_delta_set_table_properties(
        string path,
        string[] props_keys,
        string[] props_values,
        nuint props_len,
        [MarshalAs(UnmanagedType.U1)] bool raise_if_not_exists,
        string[]? keys,
        string[]? values,
        nuint cloud_len
    );
}