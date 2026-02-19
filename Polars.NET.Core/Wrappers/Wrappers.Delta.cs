using System.Runtime.InteropServices;
using Polars.NET.Core.Native;

namespace Polars.NET.Core;

public static partial class PolarsWrapper
{
    public static long Vacuum(
        string path,
        int retentionHours,
        bool enforceRetention,
        bool dryRun,
        bool vacuumModeFull,
        // Delta Cloud Options
        string[]? cloudKeys,
        string[]? cloudValues
    )
    {
        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);

        NativeBindings.pl_io_delta_vacuum(
            path,
            retentionHours,
            enforceRetention,
            dryRun,
            vacuumModeFull,
            cloudKeys,
            cloudValues,
            cloudLen,
            out var filesDeleted
        );

        ErrorHelper.CheckVoid();
        return (long)filesDeleted;
    }
    public static long Restore(
        string path,
        long targetVersion,
        long targetTimestamp,
        bool ignoreMissingFiles,
        bool protocolDowngradeAllowed,
        // Delta Cloud Options
        string[]? cloudKeys,
        string[]? cloudValues
    )
    {
        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);

        NativeBindings.pl_io_delta_restore(
            path,
            targetVersion,
            targetTimestamp,
            ignoreMissingFiles,
            protocolDowngradeAllowed,
            cloudKeys,
            cloudValues,
            cloudLen,
            out var newVersion
        );

        ErrorHelper.CheckVoid();
        return newVersion;
    }
    public static string History(
        string path,
        int limit,
        string[]? cloudKeys,
        string[]? cloudValues
    )
    {
        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);
        nuint limitNative = (nuint)(limit < 0 ? 0 : limit); // <0 or 0 means All

        IntPtr jsonPtr = IntPtr.Zero;

        try
        {
            NativeBindings.pl_io_delta_history(
                path,
                limitNative,
                cloudKeys,
                cloudValues,
                cloudLen,
                out jsonPtr
            );
            
            ErrorHelper.CheckVoid();

            string? json = Marshal.PtrToStringUTF8(jsonPtr);
            return json ?? "[]";
        }
        finally
        {
            if (jsonPtr != IntPtr.Zero)
            {
                NativeBindings.pl_free_string(jsonPtr);
            }
        }
    }
    public static ulong Optimize(
        string path,
        long targetSizeMb,
        string? filterJson,
        string[]? zOrderCols,
        // Cloud Options
        PlCloudProvider cloudProvider,
        UIntPtr cloudRetries,
        ulong cloudRetryTimeoutMs,
        ulong cloudRetryInitBackoffMs,
        ulong cloudRetryMaxBackoffMs,
        ulong cloudCacheTtl,
        string[]? cloudKeys,
        string[]? cloudValues
    )
    {
        nuint zOrderLen = (nuint)(zOrderCols?.Length ?? 0);
        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);

        nuint optimizedFilesCount;

        // 3. 调用 Native Binding
        NativeBindings.pl_io_delta_optimize(
            path,
            targetSizeMb,
            filterJson,
            zOrderCols,
            zOrderLen,
            cloudProvider,
            cloudRetries,
            cloudRetryTimeoutMs,
            cloudRetryInitBackoffMs,
            cloudRetryMaxBackoffMs,
            cloudCacheTtl,
            cloudKeys,
            cloudValues,
            cloudLen,
            out optimizedFilesCount
        );

        ErrorHelper.CheckVoid();

        return optimizedFilesCount;
    }
    public static void AddFeature(
        string path,
        string featureName,
        bool allowProtocolIncrease,
        // Cloud Options
        string[]? cloudKeys,
        string[]? cloudValues
    )
    {
        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);

        NativeBindings.pl_io_delta_add_feature(
            path,
            featureName,
            allowProtocolIncrease,
            cloudKeys,
            cloudValues,
            cloudLen
        );

        ErrorHelper.CheckVoid();
    }
    public static void SetTableProperties(
        string path,
        string[] propKeys,
        string[] propValues,
        bool raiseIfNotExists,
        // Cloud Options
        string[]? cloudKeys,
        string[]? cloudValues
    )
    {
        nuint propLen = (nuint)propKeys.Length;
        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);

        NativeBindings.pl_io_delta_set_table_properties(
            path,
            propKeys,
            propValues,
            propLen,
            raiseIfNotExists,
            cloudKeys,
            cloudValues,
            cloudLen
        );

        ErrorHelper.CheckVoid();
    }
}