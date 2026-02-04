using System.Data;
using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.FSharp.Core;

namespace Polars.NET.Core.Arrow;

public static class ArrowTypeResolver
{
    // =================================================================================
    // Type Resolver 
    // =================================================================================
    public static MemberInfo[] GetReadableMembers(Type type)
    {
        var flags = BindingFlags.Public | BindingFlags.Instance;
        
        var properties = type.GetProperties(flags)
            .Where(p => p.GetIndexParameters().Length == 0)
            .Where(p => !p.PropertyType.IsInterface && !p.PropertyType.IsAbstract)
            .Where(p => p.PropertyType != typeof(IntPtr) && p.PropertyType != typeof(UIntPtr))
            .Cast<MemberInfo>();

        var fields = type.GetFields(flags)
            .Where(f => !f.FieldType.IsInterface && !f.FieldType.IsAbstract)
            .Where(f => f.FieldType != typeof(IntPtr) && f.FieldType != typeof(UIntPtr))
            .Cast<MemberInfo>();

        return properties.Concat(fields).ToArray();
    }

    public static Type GetMemberType(MemberInfo member)
    {
        return member switch
        {
            PropertyInfo p => p.PropertyType,
            FieldInfo f => f.FieldType,
            _ => throw new NotSupportedException($"Member {member.Name} is not a Property or Field.")
        };
    }

    public static Field ResolveField(string name, Type type)
    {
        // F# Option type here
        bool isFSharpOption = type.IsGenericType && type.GetGenericTypeDefinition() == typeof(FSharpOption<>);
        Type coreType;
        if (isFSharpOption)
        {
            // Unwrap FSharpOption<T> -> T
            coreType = type.GetGenericArguments()[0];
        }
        else
        {
            // Unwrap Nullable<T> -> T
            coreType = Nullable.GetUnderlyingType(type) ?? type;
        }

        // Determine Nullability
        // Reference types, Nullables, and F# Options are always nullable in Arrow Schema
        bool isNullable = !type.IsValueType || Nullable.GetUnderlyingType(type) != null || isFSharpOption;

        // Get Arrow Type Recursively
        IArrowType arrowType = GetArrowTypeRecursively(coreType);

        return new Field(name, arrowType, isNullable);
    }

    private static IArrowType GetArrowTypeRecursively(Type type)
    {
        if (type == typeof(int))    return Int32Type.Default;
        if (type == typeof(long))   return Int64Type.Default;
        if (type == typeof(string)) return StringViewType.Default;
        if (type == typeof(bool))   return BooleanType.Default;
        if (type == typeof(double)) return DoubleType.Default;
        if (type == typeof(float))  return FloatType.Default;
        
        if (type == typeof(decimal)) return new Decimal128Type(38, 18); 
        if (type == typeof(DateTime)) return new TimestampType(TimeUnit.Microsecond, null as string); 
        if (type == typeof(TimeSpan)) return DurationType.FromTimeUnit(TimeUnit.Microsecond);
        if (type == typeof(TimeOnly)) return new Time64Type(TimeUnit.Microsecond);
        if (type == typeof(DateOnly)) return Date32Type.Default;
        if (type == typeof(DateTimeOffset)) return new TimestampType(TimeUnit.Microsecond, "UTC");

        if (type == typeof(short))  return Int16Type.Default;
        if (type == typeof(sbyte))  return Int8Type.Default;
        if (type == typeof(byte))   return UInt8Type.Default;
        if (type == typeof(ulong))  return UInt64Type.Default;
        if (type == typeof(uint))   return UInt32Type.Default;
        if (type == typeof(ushort)) return UInt16Type.Default;

        // --- (List<T>, Array) ---
        if (typeof(System.Collections.IEnumerable).IsAssignableFrom(type) && type != typeof(string))
        {
            Type elementType = GetEnumerableElementType(type)!;
            if (elementType != null)
            {
                // Recursive resolution for inner item (handles List<Option<int>> etc.)
                var innerField = ResolveField("item", elementType);
                return new LargeListType(innerField);
            }
        }

        // --- Nested Struct / Class ---
        if (!type.IsPrimitive && !type.IsEnum)
        {
            var members = GetReadableMembers(type);
            if (members.Length > 0)
            {
                var fields = members.Select(m => ResolveField(m.Name, GetMemberType(m))).ToList();
                return new StructType(fields);
            }
        }

        throw new NotSupportedException($"Type {type.Name} is not supported.");
    }

    public static Type? GetEnumerableElementType(Type type)
    {
        if (type.IsArray) return type.GetElementType();
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>)) return type.GetGenericArguments()[0];
        
        var ienum = type.GetInterfaces().FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));
        return ienum?.GetGenericArguments()[0];
    }
    public static Schema GetSchemaFromDataReader(IDataReader reader)
    {
        var fields = new List<Apache.Arrow.Field>();
        
        for (int i = 0; i < reader.FieldCount; i++)
        {
            var name = reader.GetName(i);
            var netType = reader.GetFieldType(i);

            var field = ResolveField(name, netType);
            
            fields.Add(field);
        }

        return new Schema(fields, null);
    }
    public static Type GetNetTypeFromArrowType(IArrowType arrowType)
    {
        return arrowType switch
        {
            Int32Type => typeof(int),
            Int64Type => typeof(long),
            FloatType => typeof(float),
            DoubleType => typeof(double),
            BooleanType => typeof(bool),

            // Decimal
            Decimal128Type => typeof(decimal),
            Decimal256Type => typeof(decimal),
            // String
            StringType => typeof(string),
            LargeStringType => typeof(string),
            StringViewType => typeof(string),

            // Time
            TimestampType ts => string.IsNullOrEmpty(ts.Timezone) 
                ? typeof(DateTime) 
                : typeof(DateTimeOffset),
            Date32Type => typeof(DateOnly),
            Date64Type => typeof(DateTime),
            Time32Type => typeof(TimeOnly),
            Time64Type => typeof(TimeOnly),
            DurationType => typeof(TimeSpan),

            // Binary
            BinaryType => typeof(byte[]),
            LargeBinaryType => typeof(byte[]),
            FixedSizeBinaryType => typeof(byte[]),
            BinaryViewType => typeof(byte[]),
            
            // Nested (Generic fallback)
            ListType => typeof(List<object>),
            LargeListType => typeof(List<object>),
            FixedSizeListType => typeof(List<object>),
            StructType => typeof(Dictionary<string, object>),
            
            _ => typeof(object)
        };
    }
}