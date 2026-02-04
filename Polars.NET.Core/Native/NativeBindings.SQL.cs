using System.Runtime.InteropServices;

namespace Polars.NET.Core.Native;

unsafe internal partial class NativeBindings
{
    // SQL Context
    [LibraryImport(LibName)] 
    public static partial SqlContextHandle pl_sql_context_new();

    [LibraryImport(LibName)] 
    public static partial void pl_sql_context_free(IntPtr ptr);

    [LibraryImport(LibName)] 
    public static partial void pl_sql_context_register(SqlContextHandle ctx, IntPtr name, LazyFrameHandle lf);

    [LibraryImport(LibName)] 
    public static partial LazyFrameHandle pl_sql_context_execute(SqlContextHandle ctx, IntPtr query);
}