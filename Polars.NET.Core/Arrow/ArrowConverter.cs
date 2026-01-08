using System.Linq.Expressions;
using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Polars.NET.Core.Arrow;
public static class ArrowConverter
{
    // Cache Generic Type method definition to avoid reflection in every cycle
    private static readonly MethodInfo _buildMethodDef = typeof(ArrowConverter)
        .GetMethod("Build", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("ArrowConverter.Build<T> method not found.");

    /// <summary>
    /// Read object typeinfo by reflection and transform to Arrow Arrays。
    /// </summary>
    public static List<(string Name, IArrowArray Array)> BuildColumnsFromObject(object columns)
    {
        ArgumentNullException.ThrowIfNull(columns);

        var props = columns.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);
        if (props.Length == 0)
            throw new ArgumentException("The provided object has no public properties to treat as columns.");

        var result = new List<(string, IArrowArray)>(props.Length);

        foreach (var prop in props)
        {
            var colName = prop.Name;
            var colValue = prop.GetValue(columns) ?? throw new ArgumentNullException($"Column '{colName}' cannot be null.");

            // 1. Get element type T
            Type elemType = ReflectionHelper.GetEnumerableElementType(colValue.GetType()) ?? throw new ArgumentException($"Property '{colName}' is not an IEnumerable<T> or Array.");

            // 2. Build Generic type methond as ArrowConverter.Build<T>
            var buildMethod = _buildMethodDef.MakeGenericMethod(elemType);

            // 3. Call IArrowArray
            try 
            {
                var arrowArray = (IArrowArray)buildMethod.Invoke(null, new[]{colValue})!;
                result.Add((colName, arrowArray));
            }
            catch (TargetInvocationException ex)
            {
                throw ex.InnerException ?? ex;
            }
        }
        return result;
    }
    /// <summary>
    /// General Entry：Decide which type of Arrary based on the type of T
    /// </summary>
    public static IArrowArray Build<T>(IEnumerable<T> data)
    {
        var type = typeof(T);
        // 0. Unwrap F# Option type
        if (FSharpHelper.IsFSharpOption(type))
        {
            var innerType = FSharpHelper.GetUnderlyingType(type);
            
            // Get the unwrapper for Option<U>
            var unwrapper = FSharpHelper.CreateOptionUnwrapper(type);

            // Call HandleFSharpOption<InnerType> dynamically
            var method = typeof(ArrowConverter)
                .GetMethod(nameof(HandleFSharpOption), BindingFlags.NonPublic | BindingFlags.Static)!
                .MakeGenericMethod(innerType);

            return (IArrowArray)method.Invoke(null,[data, unwrapper])!;
        }
        var underlyingType = Nullable.GetUnderlyingType(type) ?? type;
        Type checkType = underlyingType ?? type;

        // Primitives & String
        if (checkType == typeof(Half)) return BuildFloat16(data.Cast<Half?>());
        if (checkType == typeof(float)) return BuildFloat32(data.Cast<float?>());
        if (checkType == typeof(double)) return BuildDouble(data.Cast<double?>());
        if (checkType == typeof(sbyte)) return BuildInt8(data.Cast<sbyte?>());
        if (checkType == typeof(byte)) return BuildUInt8(data.Cast<byte?>());
        if (checkType == typeof(short)) return BuildInt16(data.Cast<short?>());
        if (checkType == typeof(ushort)) return BuildUInt16(data.Cast<ushort?>());
        if (checkType == typeof(int)) return BuildInt32(data.Cast<int?>());
        if (checkType == typeof(long)) return BuildInt64(data.Cast<long?>());
        if (checkType == typeof(uint)) return BuildUInt32(data.Cast<uint?>());
        if (checkType == typeof(ulong)) return BuildUInt64(data.Cast<ulong?>());
        if (checkType == typeof(bool)) return BuildBoolean(data.Cast<bool?>());
        if (checkType == typeof(string)) return BuildString(data.Cast<string?>());
        if (checkType == typeof(DateOnly)) return BuildDate32(data.Cast<DateOnly?>());
        if (checkType == typeof(TimeOnly)) return BuildTime64(data.Cast<TimeOnly?>());
        if (checkType == typeof(DateTime)) return BuildTimestamp(data.Cast<DateTime?>());
        if (checkType == typeof(DateTimeOffset)) return BuildDateTimeOffset(data.Cast<DateTimeOffset?>());
        if (checkType == typeof(TimeSpan)) return BuildDuration(data.Cast<TimeSpan?>());
        if (checkType == typeof(decimal)) return BuildDecimal(data.Cast<decimal?>());
        // Mixed type/Object
        // Treat as String
        if (checkType == typeof(object))
        {
            var stringBuilder = new StringViewArray.Builder();
            foreach (var item in data)
            {
                if (item == null)
                {
                    stringBuilder.AppendNull();
                }
                else
                {
                    stringBuilder.Append(item.ToString());
                }
            }
            return stringBuilder.Build();
        }
        // List<U>
        // Check whether IEnumerable<U> is available and not string type
        if (type != typeof(string) && 
            type.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>)))
        {
            // Get List innertype U
            var enumerableInterface = type.GetInterfaces()
                .First(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));
            var elementType = enumerableInterface.GetGenericArguments()[0];

            // Use reflection method BuildListArray<U>
            var method = typeof(ArrowConverter)
                .GetMethod(nameof(BuildListArray), BindingFlags.Public | BindingFlags.Static)!
                .MakeGenericMethod(elementType);

            return (IArrowArray)method.Invoke(null, new[]{data})!;
        }

        // Struct
        if (type.IsClass)
        {
            return StructBuilderHelper.BuildStructArray(data);
        }

        if (IsKeyValuePair(type))
        {
            return StructBuilderHelper.BuildStructArray(data);
        }
        throw new NotSupportedException($"Type {type.FullName} (Underlying: {checkType.FullName}) is not supported yet.");
    }

    private static bool IsKeyValuePair(Type t)
    {
        return t.IsGenericType && t.GetGenericTypeDefinition() == typeof(KeyValuePair<,>);
    }

    /// <summary>
    /// Build ListArray 
    /// </summary>
    public static LargeListArray BuildListArray<U>(IEnumerable<IEnumerable<U>?> data)
    {
        var flattenedData = new List<U>();
        
        var offsetsBuilder = new Int64Array.Builder();
        var validityBuilder = new BooleanArray.Builder();
        
        long currentOffset = 0;
        offsetsBuilder.Append(0); 

        int nullCount = 0;

        foreach (var subList in data)
        {
            if (subList == null)
            {
                validityBuilder.Append(false);
                offsetsBuilder.Append(currentOffset);
                nullCount++;
            }
            else
            {
                validityBuilder.Append(true);
                
                int count = 0;
                foreach (var item in subList)
                {
                    flattenedData.Add(item);
                    count++;
                }
                
                currentOffset += count;
                offsetsBuilder.Append(currentOffset);
            }
        }

        IArrowArray valuesArray = Build(flattenedData);

        var offsetsArray = offsetsBuilder.Build();
        var validityArray = validityBuilder.Build();
        
        var listType = new LargeListType(valuesArray.Data.DataType);

        return new LargeListArray(
            listType,
            data.Count(),
            offsetsArray.ValueBuffer,
            valuesArray,
            validityArray.ValueBuffer,
            nullCount
        );
    }

    /// <summary>
    /// Build FixedSizeListArray
    /// </summary>
    public static FixedSizeListArray BuildFixedSizeListArray<U>(IEnumerable<IEnumerable<U>?> data, int listSize)
    {
        var flattenedData = new List<U>();
        var validityBuilder = new BooleanArray.Builder();
        int nullCount = 0;
        int rowCount = 0;

        foreach (var subList in data)
        {
            rowCount++;
            if (subList == null)
            {
                validityBuilder.Append(false);
                nullCount++;
                for(int i=0; i<listSize; i++) flattenedData.Add(default!);
            }
            else
            {
                int count = 0;
                foreach (var item in subList)
                {
                    flattenedData.Add(item);
                    count++;
                }

                if (count != listSize)
                    throw new ArgumentException($"Element at index {rowCount-1} has length {count}, expected {listSize}.");

                validityBuilder.Append(true);
            }
        }

        IArrowArray valuesArray = Build(flattenedData);
        var validityArray = validityBuilder.Build();

        var listType = new FixedSizeListType(valuesArray.Data.DataType, listSize);

        return new FixedSizeListArray(
            listType,
            rowCount,
            valuesArray,
            validityArray.ValueBuffer,
            nullCount
        );
    }

    // --- Primitive Type Builders ---
    /// <summary>
    /// Helper：Check whether U is value type or ref type
    /// </summary>
    private static IArrowArray HandleFSharpOption<U>(IEnumerable<object> data, Func<object, object?> unwrapper)
    {
        if (typeof(U).IsValueType)
        {
            var method = typeof(ArrowConverter)
                .GetMethod(nameof(BuildStructOption), BindingFlags.NonPublic | BindingFlags.Static)!
                .MakeGenericMethod(typeof(U));
            return (IArrowArray)method.Invoke(null, new object[]{data, unwrapper})!;
        }
        else
        {
            return BuildClassOption<U>(data, unwrapper);
        }
    }

    /// <summary>
    /// Deal with Value Type Option
    /// </summary>
    private static IArrowArray BuildStructOption<T>(IEnumerable<object> data, Func<object, object?> unwrapper) 
        where T : struct
    {
        // Convert IEnumerable<FSharpOption<T>> To IEnumerable<T?>
        var nullableData = data.Select(item => 
        {
            var val = unwrapper(item);
            return val == null ? (T?)null : (T)val;
        });
        
        return Build(nullableData);
    }

    /// <summary>
    /// Deal with Ref Type Option
    /// </summary>
    private static IArrowArray BuildClassOption<T>(IEnumerable<object> data, Func<object, object?> unwrapper)
    {
        var classData = data.Select(item => 
        {
            var val = unwrapper(item);
            return (T)val!; 
        });

        return Build(classData);
    }
    private static HalfFloatArray BuildFloat16(IEnumerable<Half?> data)
    {
        var b = new HalfFloatArray.Builder();
        foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
        return b.Build();
    }

    private static FloatArray BuildFloat32(IEnumerable<float?> data)
    {
        var b = new FloatArray.Builder();
        foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
        return b.Build();
    }

    private static Decimal128Array BuildDecimal(IEnumerable<decimal?> data)
    {
        var type = new Decimal128Type(38, 18); 
        var b = new Decimal128Array.Builder(type);
        foreach (var v in data)
        {
            if (v.HasValue) b.Append(v.Value);
            else b.AppendNull();
        }
        return b.Build();
    }
    private static Int8Array BuildInt8(IEnumerable<sbyte?> data)
    {
        var b = new Int8Array.Builder();
        foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
        return b.Build();
    }

    private static UInt8Array BuildUInt8(IEnumerable<byte?> data)
    {
        var b = new UInt8Array.Builder();
        foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
        return b.Build();
    }

    private static Int16Array BuildInt16(IEnumerable<short?> data)
    {
        var b = new Int16Array.Builder();
        foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
        return b.Build();
    }

    private static UInt16Array BuildUInt16(IEnumerable<ushort?> data)
    {
        var b = new UInt16Array.Builder();
        foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
        return b.Build();
    }    

    private static Int32Array BuildInt32(IEnumerable<int?> data)
    {
        var b = new Int32Array.Builder();
        foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
        return b.Build();
    }
    
    private static Int64Array BuildInt64(IEnumerable<long?> data)
    {
        var b = new Int64Array.Builder();
        foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
        return b.Build();
    }
    private static UInt32Array BuildUInt32(IEnumerable<uint?> data)
    {
        var b = new UInt32Array.Builder();
        foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
        return b.Build();
    }
    
    private static UInt64Array BuildUInt64(IEnumerable<ulong?> data)
    {
        var b = new UInt64Array.Builder();
        foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
        return b.Build();
    }

    private static DoubleArray BuildDouble(IEnumerable<double?> data)
    {
        var b = new DoubleArray.Builder();
        foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
        return b.Build();
    }

    private static BooleanArray BuildBoolean(IEnumerable<bool?> data)
    {
        var b = new BooleanArray.Builder();
        foreach (var v in data) if (v.HasValue) b.Append(v.Value); else b.AppendNull();
        return b.Build();
    }

    private static StringViewArray BuildString(IEnumerable<string?> data)
    {
        var b = new StringViewArray.Builder();
        foreach (var v in data) b.Append(v);
        return b.Build();
    }
    // DateOnly -> Date32 (Days since epoch)
    private static Date32Array BuildDate32(IEnumerable<DateOnly?> data)
    {
        var b = new Date32Array.Builder();
        int epoch = new DateOnly(1970, 1, 1).DayNumber;
        foreach (var v in data)
        {
            if (v.HasValue) b.Append(v.Value.ToDateTime(TimeOnly.MinValue));
            else b.AppendNull();
        }
        return b.Build();
    }

    // TimeOnly -> Time64 (Microseconds)
    private static Time64Array BuildTime64(IEnumerable<TimeOnly?> data)
    {
        var b = new Time64Array.Builder(TimeUnit.Microsecond); // 注意设置单位
        foreach (var v in data)
        {
            if (v.HasValue) b.Append(v.Value.Ticks / 10L); // 1 tick = 100ns, 10 ticks = 1us
            else b.AppendNull();
        }
        return b.Build();
    }

    // DateTime -> Timestamp (Microsecond)
    private static TimestampArray BuildTimestamp(IEnumerable<DateTime?> data)
    {
        var b = new TimestampArray.Builder(TimeUnit.Microsecond);
        
        foreach (var v in data)
        {
            if (v.HasValue)
            {
                DateTime dt = v.Value;
                
                var dto = new DateTimeOffset(dt.Ticks, TimeSpan.Zero);

                // Ticks (100ns) -> Microsecond (1000ns)
                b.Append(dto);
            }
            else 
            {
                b.AppendNull();
            }
        }
        return b.Build();
    }
    private static TimestampArray BuildDateTimeOffset(IEnumerable<DateTimeOffset?> data)
    {
        var b = new TimestampArray.Builder(TimeUnit.Microsecond);
        
        foreach (var v in data)
        {
            if (v.HasValue)
            {
                b.Append(v.Value); 
            }
            else
            {
                b.AppendNull();
            }
        }
        return b.Build();
    }
    private static DurationArray BuildDuration(IEnumerable<TimeSpan?> data)
    {
        var b = new DurationArray.Builder(DurationType.Microsecond);

        foreach (var v in data)
        {
            if (v.HasValue)
            {
                // Ticks (100ns) -> Microseconds (1000ns)
                b.Append(v.Value.Ticks / 10L);
            }
            else
            {
                b.AppendNull();
            }
        }
        return b.Build();
    }

    internal static class StructBuilderHelper
    {
        // =================================================================
        // Columnar Construction
        // =================================================================
        public static StructArray BuildStructArray<T>(IEnumerable<T> data)
        {
            var dataList = data as IList<T> ?? data.ToList();
            int length = dataList.Count;
            var type = typeof(T);

            // Get Properties for type
            var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.GetIndexParameters().Length == 0)
                .Where(p => !p.PropertyType.IsInterface && !p.PropertyType.IsAbstract)
                .Where(p => p.PropertyType != typeof(IntPtr) && p.PropertyType != typeof(UIntPtr))
                .ToArray();

            var fields = new List<Field>();
            var childrenArrays = new List<IArrowArray>();

            foreach (var prop in properties)
            {
                var getter = CompileGetter<T>(prop);

                var childArray = ProjectAndBuild(dataList, prop.PropertyType, getter);

                bool isNullable = !prop.PropertyType.IsValueType || Nullable.GetUnderlyingType(prop.PropertyType) != null;
                var field = new Field(prop.Name, childArray.Data.DataType, isNullable);

                fields.Add(field);
                childrenArrays.Add(childArray);
            }

            // Build Validity Bitmap for Struct itself
            var validityBuilder = new BooleanArray.Builder();
            int nullCount = 0;
            foreach (var item in dataList)
            {
                if (item == null)
                {
                    validityBuilder.Append(false);
                    nullCount++;
                }
                else
                {
                    validityBuilder.Append(true);
                }
            }
            var validityBuffer = validityBuilder.Build().ValueBuffer;

            var structType = new StructType(fields);
            
            return new StructArray(
                structType,
                length,
                childrenArrays,
                validityBuffer,
                nullCount
            );
        }

        // =================================================================
        // Helper：Reflection Bridge
        // =================================================================
        
        /// <summary>
        /// Convert IList<TParent> To IEnumerable<TProp>, then call ArrowConverter.Build
        /// </summary>
        private static IArrowArray ProjectAndBuild<TParent>(IList<TParent> data, Type propType, Func<TParent, object?> getter)
        {
            bool isFSharpOption = FSharpHelper.IsFSharpOption(propType);
            
            Type cleanType = propType;

            if (isFSharpOption)
            {
                cleanType = FSharpHelper.GetUnderlyingType(propType);
            }
            else
            {
                cleanType = Nullable.GetUnderlyingType(propType) ?? propType;
            }

            // For value type (int, double)，let it be Nullable (int?, double?)
            // For ref type (string, List), keep it as is
            Type targetType = cleanType;
            if (cleanType.IsValueType)
            {
                targetType = typeof(Nullable<>).MakeGenericType(cleanType);
            }

            var method = typeof(StructBuilderHelper)
                .GetMethod(nameof(BuildColumn), BindingFlags.NonPublic | BindingFlags.Static)!
                .MakeGenericMethod(typeof(TParent), targetType);

            return (IArrowArray)method.Invoke(null,new object[] {data, getter, isFSharpOption})!;
        }

        /// <summary>
        /// Data projection and build method
        /// </summary>
        private static IArrowArray BuildColumn<TParent, TProp>(
            IList<TParent> data, 
            Func<TParent, object?> getter, 
            bool isOption)
        {
            Func<object, object?>? unwrapper = null;
            if (isOption)
            {
                // Get TProp Underlying type (e.g. int?, double?)
                var innerType = Nullable.GetUnderlyingType(typeof(TProp)) ?? typeof(TProp);
                
                // Build FSharpOption<Inner> Type
                var optionType = FSharpHelper.MakeFSharpOptionType(innerType);
                
                unwrapper = FSharpHelper.CreateOptionUnwrapper(optionType);
            }
            // Projection
            // Convert List<Parent> to IEnumerable<Prop>
            var columnData = data.Select(item => 
            {
                if (item == null) return default;

                var rawVal = getter(item);
                
                if (rawVal == null) return default;
                if (isOption)
                {
                    var unwrapped = unwrapper!(rawVal);
                    if (unwrapped == null) return default;
                    return (TProp)unwrapped;
                }
                return (TProp)rawVal;
            });
            // Call common converter
            // Auto routing:
            // - TProp is int -> BuildInt32
            // - TProp is NestedItem -> BuildStructArray (Recurring)
            // - TProp is List<double> -> BuildListArray
            return ArrowConverter.Build(columnData);
        }

        // =================================================================
        // Compile Expression Tree for Getter
        // =================================================================
        private static Func<T, object?> CompileGetter<T>(PropertyInfo propertyInfo)
        {
            var instanceParam = Expression.Parameter(typeof(T), "item");
            var propertyAccess = Expression.Property(instanceParam, propertyInfo);
            var convertToObject = Expression.Convert(propertyAccess, typeof(object));
            return Expression.Lambda<Func<T, object?>>(convertToObject, instanceParam).Compile();
        }
    }
}
