#pragma warning disable CS1591
using Polars.NET.Core;

namespace Polars.CSharp;

/// <summary>
/// Represents a Polars Schema (Name -> DataType mapping).
/// </summary>
public class PolarsSchema : IDisposable
{
    internal SchemaHandle Handle { get; private set; }
    private bool _disposed;

    /// <summary>
    /// Internal constructor: Wrap an existing handle (e.g. from Rust return).
    /// </summary>
    internal PolarsSchema(SchemaHandle handle)
    {
        Handle = handle;
    }

    /// <summary>
    /// Create a new empty Schema.
    /// </summary>
    public PolarsSchema()
    {
        Handle = PolarsWrapper.SchemaCreate();
    }

    /// <summary>
    /// Create a Schema from a Dictionary.
    /// </summary>
    public static PolarsSchema From(Dictionary<string, DataType> fields)
    {
        var schema = new PolarsSchema();
        foreach (var kvp in fields)
        {
            schema.Add(kvp.Key, kvp.Value);
        }
        return schema;
    }

    /// <summary>
    /// Add a field to the schema.
    /// </summary>
    /// <param name="name">Column name.</param>
    /// <param name="dtype">Column data type.</param>
    /// <returns>The schema instance (Fluent API).</returns>
    public PolarsSchema Add(string name, DataType dtype)
    {
        if (dtype == null) throw new ArgumentNullException(nameof(dtype));
        
        PolarsWrapper.SchemaAddField(Handle, name, dtype.Handle);
        return this;
    }

    /// <summary>
    /// Convert the native Schema back to a Dictionary for inspection.
    /// </summary>
    public Dictionary<string, DataType> ToDictionary()
    {
        if (Handle.IsInvalid) return new Dictionary<string, DataType>();

        ulong len = PolarsWrapper.GetSchemaLen(Handle);
        var result = new Dictionary<string, DataType>((int)len);

        for (ulong i = 0; i < len; i++)
        {
            PolarsWrapper.GetSchemaFieldAt(Handle, i, out string name, out DataTypeHandle dtHandle);
            result[name] = new DataType(dtHandle);
        }

        return result;
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            Handle?.Dispose();
            _disposed = true;
        }
    }
}