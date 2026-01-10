namespace Polars.NET.Core;

public static partial class PolarsWrapper
{

    /// <summary>
    /// Build Schema from name and dtype
    /// </summary>
    public static SchemaHandle NewSchema(string[] names, DataTypeHandle[] types)
    {
        if (names.Length != types.Length)
            throw new ArgumentException("Names and Types must have same length");

        var typePtrs = HandlesToPtrs(types);

        return UseUtf8StringArray(names, (namePtrs) => 
        {
            return ErrorHelper.Check(
                NativeBindings.pl_schema_new(namePtrs, typePtrs, (UIntPtr)names.Length)
            );
        });
    }
    /// <summary>
    /// Convert Schema Dict to SchemaHandle for Rust
    /// SafeHandleLock -> Marshal Strings -> New Schema -> Action -> Dispose Schema
    /// </summary>
    private static T WithSchemaHandle<T>(
        Dictionary<string, DataTypeHandle>? schema, 
        Func<SchemaHandle, T> action)
    {
        // If Schema is blank，transfer invalid handle
        if (schema == null || schema.Count == 0)
        {
            return action(new SchemaHandle()); 
        }

        var names = schema.Keys.ToArray();
        var handles = schema.Values.ToArray();

        // Lock DataTypeHandles and get its raw pointer
        using var locker = new SafeHandleLock<DataTypeHandle>(handles);
        var typePtrs = locker.Pointers;
        
        return UseUtf8StringArray(names, namePtrs => 
        {
            using var schemaHandle = ErrorHelper.Check(
                NativeBindings.pl_schema_new(
                    namePtrs, 
                    typePtrs, 
                    (UIntPtr)names.Length
                )
            );

            return action(schemaHandle);
        });
    }
}