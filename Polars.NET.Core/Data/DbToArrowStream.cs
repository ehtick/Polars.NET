using System.Collections;
using System.Data;
using Apache.Arrow;
using Apache.Arrow.Types;
using Polars.NET.Core.Arrow;

namespace Polars.NET.Core.Data;
internal static class DbToArrowStream
{
    internal static IEnumerable<RecordBatch> ToArrowBatches(this IDataReader reader, int batchSize = 50_000)
    {
        // 1. Schema
        var schema = GetArrowSchema(reader);
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

    // Get Schema 
    internal static Schema GetArrowSchema(IDataReader reader)
    {
        {
            var fields = new List<Field>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                var name = reader.GetName(i);
                var type = reader.GetFieldType(i); 
                
                IArrowType arrowType = GetArrowType(type);
                
                fields.Add(new Field(name, arrowType, true));
            }
            return new Schema(fields, null);
        }
    }
    private static IArrowType GetArrowType(Type t)
    {
        // For Nullable<T>，like int?, shoule be recongized as int
        Type checkType = Nullable.GetUnderlyingType(t) ?? t;

        if (checkType == typeof(int)) return Int32Type.Default;
        if (checkType == typeof(long)) return Int64Type.Default;
        if (checkType == typeof(float)) return FloatType.Default;
        if (checkType == typeof(double)) return DoubleType.Default;
        if (checkType == typeof(decimal)) return DoubleType.Default;
        if (checkType == typeof(bool)) return BooleanType.Default;
        if (checkType == typeof(string)) return StringViewType.Default;
        if (checkType == typeof(DateTime)) return new TimestampType(TimeUnit.Microsecond, (string)null!);
        if (checkType == typeof(TimeSpan)) return DurationType.FromTimeUnit(TimeUnit.Microsecond);
        if (checkType == typeof(DateOnly)) return Date32Type.Default;
        if (checkType == typeof(TimeOnly)) return Time64Type.Default;

        if (checkType != typeof(string) && typeof(IEnumerable).IsAssignableFrom(checkType))
        {
            var elemType = GetEnumerableElementType(checkType);
            var childArrowType = GetArrowType(elemType);
            return new LargeListType(new Field("item", childArrowType, true));
        }

        if (checkType.IsClass || (checkType.IsValueType && !checkType.IsPrimitive && !checkType.IsEnum))
        {
            return ReflectStructSchema(checkType);
        }

        return StringViewType.Default;
    }

    // Recurring reflectiing Struct Schema
    private static StructType ReflectStructSchema(Type t)
    {
        var properties = t.GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance)
            .Where(p => p.GetIndexParameters().Length == 0)
            .Where(p => !p.PropertyType.IsInterface && !p.PropertyType.IsAbstract)
            .Where(p => p.PropertyType != typeof(IntPtr) && p.PropertyType != typeof(UIntPtr));

        var fields = new List<Field>();
        foreach (var prop in properties)
        {
            var childType = GetArrowType(prop.PropertyType);
            
            fields.Add(new Field(prop.Name, childType, true));
        }

        return new StructType(fields);
    }

    // Reflection Helper
    private static Type GetEnumerableElementType(Type type)
    {
        if (type.IsArray) return type.GetElementType()!;
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
            return type.GetGenericArguments()[0];
        foreach (var i in type.GetInterfaces())
            if (i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                return i.GetGenericArguments()[0];
        return typeof(object);
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
