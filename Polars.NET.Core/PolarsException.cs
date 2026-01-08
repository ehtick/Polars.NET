using System.Runtime.InteropServices;

namespace Polars.NET.Core;

internal static class ErrorHelper
{
    public static T Check<T>(T handle) where T : PolarsHandle
    {
        if (!handle.IsInvalid) return handle;

        IntPtr msgPtr = NativeBindings.pl_get_last_error();
        if (msgPtr == IntPtr.Zero)
        {
            throw new Exception("Polars operation failed (Unknown Error).");
        }

        try
        {
            string msg = Marshal.PtrToStringUTF8(msgPtr) ?? "Unknown Rust Error";
            throw new Exception($"[Polars Error] {msg}");
        }
        finally
        {
            NativeBindings.pl_free_error_msg(msgPtr);
        }
    }

    // For situation with void return
    public static void CheckVoid()
    {
        IntPtr msgPtr = NativeBindings.pl_get_last_error();
        if (msgPtr != IntPtr.Zero)
        {
            try
            {
                string msg = Marshal.PtrToStringUTF8(msgPtr) ?? "Unknown Rust Error";
                throw new Exception($"[Polars Void Error] {msg}");
            }
            finally
            {
                NativeBindings.pl_free_error_msg(msgPtr);
            }
        }
    }
    internal static string CheckString(IntPtr ptr)
    {
        if (ptr == IntPtr.Zero) 
        {
            CheckVoid();
            return string.Empty; 
        }
        try { return Marshal.PtrToStringUTF8(ptr) ?? ""; }
        finally { NativeBindings.pl_free_string(ptr); }
    }
}