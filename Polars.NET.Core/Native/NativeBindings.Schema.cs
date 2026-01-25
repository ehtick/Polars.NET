using System.Runtime.InteropServices;

namespace Polars.NET.Core.Native;

unsafe internal partial class NativeBindings
{
    // Schema
    [LibraryImport(LibName)]
    public static partial void pl_schema_free(IntPtr ptr);

    [LibraryImport(LibName)]
    public static partial SchemaHandle pl_schema_new(
        IntPtr[] names, 
        IntPtr[] dtypes, 
        UIntPtr len
    );
    // Introspection
    [LibraryImport(LibName)]
    public static partial UIntPtr pl_schema_len(SchemaHandle schema);

    [LibraryImport(LibName)]
    public static partial void pl_schema_get_at_index(
        SchemaHandle schema,
        UIntPtr index,
        out IntPtr namePtr,
        out DataTypeHandle dtypeHandle
    );
}