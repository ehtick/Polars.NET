using System.Collections;
using System.Data;
using Apache.Arrow;
using Apache.Arrow.Types;
using Polars.NET.Core.Arrow;

namespace Polars.NET.Core.Data;
public static class DbToArrowStream
{
    public static IEnumerable<RecordBatch> ToArrowBatches(IDataReader reader, int batchSize = 50_000)
    {
        // 1. Schema
        var schema = ArrowTypeResolver.GetSchemaFromDataReader(reader);
        var colCount = reader.FieldCount;

        // 2. Buffer Pool
        var buffers = new IColumnBuffer[colCount];
        for (int i = 0; i < colCount; i++)
        {
            Type netType = reader.GetFieldType(i);
            buffers[i] = ColumnBufferFactory.Create(netType, batchSize);
        }

        int rowCount = 0;
        bool hasYielded = false;

        // 3. Pump
        while (reader.Read())
        {
            for (int i = 0; i < colCount; i++)
            {
                buffers[i].Add(reader.GetValue(i));
            }

            rowCount++;

            if (rowCount >= batchSize)
            {
                yield return Flush(schema, buffers, rowCount);
                rowCount = 0;
                hasYielded = true;
            }
        }


        // 4. End
        if (rowCount > 0)
        {
            yield return Flush(schema, buffers, rowCount);
            hasYielded = true;
        }
        
        // 5. Check null stream
        if (!hasYielded)
        {
            yield return Flush(schema, buffers, 0);
        }
    }

    private static RecordBatch Flush(Schema schema, IColumnBuffer[] buffers, int length)
    {
        var arrays = new List<IArrowArray>(buffers.Length);
        for (int i = 0; i < buffers.Length; i++)
        {
            var buffer = buffers[i];
            
            if (buffer.Count != length)
            {
                Console.WriteLine($"[CRITICAL PANIC PREDICTION] Column {i} ('{schema.GetFieldByIndex(i).Name}') has {buffer.Count} elements, but we are creating a batch of length {length}!");
            }

            var array = buffer.BuildArray();
            
            if (array.Length != length)
            {
                Console.WriteLine($"[CRITICAL ARROW ERROR] Column {i} BuildArray() returned length {array.Length}, expected {length}. ArrowConverter might be adding extras?");
            }

            arrays.Add(array);
            buffer.Clear();
        }
        return new RecordBatch(schema, arrays, length);
    }
}

// =========================================================
// Glue Layer：Connect DataReader(object) to ArrowConverter(T)
// =========================================================

public interface IColumnBuffer
{
    void Add(object value);
    void Clear();
    IArrowArray BuildArray();
    int Count { get; }
}

public static class ColumnBufferFactory
{
    public static IColumnBuffer Create(Type netType, int capacity)
    {
        Type targetType = netType;

        if (netType.IsValueType && Nullable.GetUnderlyingType(netType) == null)
        {
            targetType = typeof(Nullable<>).MakeGenericType(netType);
        }
        
        var bufferType = typeof(TypedColumnBuffer<>).MakeGenericType(targetType);
        return (IColumnBuffer)Activator.CreateInstance(bufferType, capacity)!;
    }
}

internal class TypedColumnBuffer<T> : IColumnBuffer
{
    private readonly List<T> _buffer;
    private static readonly Func<IEnumerable<T>, IArrowArray> _converter;
    

    static TypedColumnBuffer()
    {
        var method = typeof(ArrowConverter)
            .GetMethod("Build", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic)
            ?.MakeGenericMethod(typeof(T));
            
        if (method == null) throw new InvalidOperationException($"ArrowConverter.Build<{typeof(T).Name}> not found.");
        
        _converter = (Func<IEnumerable<T>, IArrowArray>)Delegate.CreateDelegate(typeof(Func<IEnumerable<T>, IArrowArray>), method);
    }

    public TypedColumnBuffer(int capacity)
    {
        _buffer = new List<T>(capacity);
    }

    public void Add(object value)
    {
        if (value == null || value == DBNull.Value)
            _buffer.Add(default!);
        else
            _buffer.Add((T)value); // Unbox
    }

    public void Clear() => _buffer.Clear();

    public IArrowArray BuildArray() => _converter(_buffer);
    // Debug
    public int Count => _buffer.Count;
}
