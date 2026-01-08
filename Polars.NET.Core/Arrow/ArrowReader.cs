using System.Collections;
using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Polars.NET.Core.Arrow
{
    public static class ArrowReader
    {
        public static IEnumerable<T> ReadRecordBatch<T>(RecordBatch batch)
        {
            
            var targetType = typeof(T);

            // Scalar Mode
            if (IsScalarType(targetType))
            {
                if (batch.ColumnCount == 0) yield break;

                var col = batch.Column(0);
                var accessor = CreateAccessor(col, targetType);
                int count = batch.Length;

                for (int i = 0; i < count; i++)
                {
                    var val = accessor(i);
                    yield return val == null ? default! : (T)val;
                }
                yield break;
            }
            // Object Mapping Mode
            // For POCO (class/struct)
            
            int rowCount = batch.Length;
            
            // Get Can-write properties
            var properties = targetType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                                       .Where(p => p.CanWrite).ToArray();
            
            var columnAccessors = new Func<int, object?>[properties.Length];

            for (int i = 0; i < properties.Length; i++)
            {
                var prop = properties[i];
                var col = batch.Column(prop.Name); 

                if (col == null) 
                {
                    columnAccessors[i] = _ => null; 
                    continue; 
                }

                columnAccessors[i] = CreateAccessor(col, prop.PropertyType);
            }

            for (int i = 0; i < rowCount; i++)
            {
                var item = Activator.CreateInstance<T>()!; 
                
                for (int p = 0; p < properties.Length; p++)
                {
                    var accessor = columnAccessors[p];
                    var val = accessor(i);
                    if (val != null) properties[p].SetValue(item, val);
                }
                yield return item;
            }
        }

        // =============================================================
        // Accessor Factory
        // =============================================================
        public static Func<int, object?> CreateAccessor(IArrowArray array, Type targetType)
        {
            // ---------------------------------------------------------
            // Type Resolution
            // ---------------------------------------------------------
            bool isFSharpOption = FSharpHelper.IsFSharpOption(targetType);
            
            // Get Real Underlying Type
            // Option<int> -> int
            // int?        -> int
            // List<T>     -> List<T>
            var underlyingType = isFSharpOption 
                ? FSharpHelper.GetUnderlyingType(targetType) 
                : (Nullable.GetUnderlyingType(targetType) ?? targetType);

            // Define baseAccessor(return C# object or null)
            Func<int, object?> baseAccessor = null!;

            // ---------------------------------------------------------
            // StructArray -> Class / Struct
            // ---------------------------------------------------------
            if (array is StructArray structArray)
            {
                var props = underlyingType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                                          .Where(p => p.CanWrite).ToArray();
                
                var structType = (StructType)structArray.Data.DataType;
                var setters = new List<Action<object, int>>();

                foreach (var prop in props)
                {
                    int fieldIndex = -1;
                    for (int k = 0; k < structType.Fields.Count; k++)
                    {
                        if (structType.Fields[k].Name == prop.Name) { fieldIndex = k; break; }
                    }

                    if (fieldIndex == -1) continue;

                    var childArray = structArray.Fields[fieldIndex];
                    
                    var childGetter = CreateAccessor(childArray, prop.PropertyType);

                    setters.Add((obj, rowIdx) => 
                    {
                        var val = childGetter(rowIdx);
                        if (val != null) prop.SetValue(obj, val);
                    });
                }

                baseAccessor = idx => 
                {
                    if (structArray.IsNull(idx)) return null;
                    var instance = Activator.CreateInstance(underlyingType)!;
                    foreach (var setter in setters) setter(instance, idx);
                    return instance;
                };
            }
            // ---------------------------------------------------------
            // ListArray / LargeListArray -> List<T> / FixedSizeListArray -> List<T>
            // ---------------------------------------------------------
            else if (array is ListArray || array is LargeListArray || array is FixedSizeListArray)
            {
                Type elementType = typeof(object);
                if (underlyingType.IsGenericType) elementType = underlyingType.GetGenericArguments()[0];
                else if (underlyingType.IsArray) elementType = underlyingType.GetElementType()!;
                bool isFSharpList = underlyingType.IsGenericType && 
                                    (underlyingType.GetGenericTypeDefinition().FullName == "Microsoft.FSharp.Collections.FSharpList`1");

                IArrowArray valuesArray;
                Func<int, long> getOffset;
                Func<int, bool> isNull;

                if (array is ListArray listArr)
                {
                    valuesArray = listArr.Values;
                    getOffset = i => listArr.ValueOffsets[i];
                    isNull = listArr.IsNull;
                }
                else if (array is LargeListArray largeArr)
                {
                    valuesArray = largeArr.Values;
                    getOffset = i => largeArr.ValueOffsets[i];
                    isNull = largeArr.IsNull;
                }
                else
                {
                    var fixedArr = (FixedSizeListArray)array;
                    valuesArray = fixedArr.Values;
                    int width = ((FixedSizeListType)fixedArr.Data.DataType).ListSize;
                    getOffset = i =>  (long)(i + array.Offset) * width;
                    isNull = fixedArr.IsNull;
                }

                var childGetter = CreateAccessor(valuesArray, elementType);

                baseAccessor = idx =>
                {
                    if (isNull(idx)) return null;

                    long start = getOffset(idx);
                    long end = getOffset(idx + 1);
                    int count = (int)(end - start);

                    var listType = typeof(List<>).MakeGenericType(elementType);
                    var list = (IList)Activator.CreateInstance(listType, count)!;

                    for (int k = 0; k < count; k++)
                    {
                        var val = childGetter((int)(start + k));
                        list.Add(val);
                    }

                    if (underlyingType.IsArray)
                    {
                        var arr = System.Array.CreateInstance(elementType, list.Count);
                        list.CopyTo(arr, 0);
                        return arr;
                    }
                    if (isFSharpList)
                    {
                        return FSharpHelper.ToFSharpList(list, elementType);
                    }
                    return list;
                };
            }
            // ---------------------------------------------------------
            // Primitives
            // ---------------------------------------------------------
            else
            {
                if (underlyingType == typeof(string))
                    baseAccessor = array.GetStringValue;

                else if (underlyingType == typeof(int) || underlyingType == typeof(long) 
                || underlyingType == typeof(uint) || underlyingType == typeof(ushort)
                || underlyingType == typeof(ulong) || underlyingType == typeof(sbyte)
                || underlyingType == typeof(byte) || underlyingType == typeof(short))
                {
                    baseAccessor = idx => 
                    {
                        long? val = array.GetInt64Value(idx);
                        if (!val.HasValue) return null;
                        if (underlyingType == typeof(sbyte)) return (sbyte)val.Value;
                        if (underlyingType == typeof(byte))  return (byte)val.Value;
                        if (underlyingType == typeof(short)) return (short)val.Value;
                        if (underlyingType == typeof(int)) return (int)val.Value;
                        if (underlyingType == typeof(uint)) return (uint)val.Value;
                        if (underlyingType == typeof(ushort)) return (ushort)val.Value;
                        if (underlyingType == typeof(ulong)) return (ulong)val.Value;
                        return val.Value;
                    };
                }

                else if (underlyingType == typeof(double) || underlyingType == typeof(float) || underlyingType == typeof(Half))
                {
                    baseAccessor = idx => 
                    {
                        double? v = array.GetDoubleValue(idx);
                        if (!v.HasValue) return null;
                        if (underlyingType == typeof(float)) return (float)v.Value;
                        if (underlyingType == typeof(Half)) return (Half)v.Value;
                        return v.Value;
                    };
                }

                else if (underlyingType == typeof(decimal))
                {
                    baseAccessor = idx =>
                    {
                        if (array is Decimal128Array decArr) return decArr.GetValue(idx);
                        if (array is DoubleArray dArr) return dArr.GetValue(idx) is double v ? (decimal)v : (decimal?)null;
                        return null;
                    };
                }

                else if (underlyingType == typeof(bool))
                {
                    baseAccessor = idx => (array as BooleanArray)?.GetValue(idx);
                }

                else if (underlyingType == typeof(DateTime))
                {
                    baseAccessor = idx => array.GetDateTime(idx);
                }
                else if (underlyingType == typeof(DateTimeOffset))
                {
                TimeZoneInfo? tzi = null;
                
                if (array is TimestampArray tsArr && tsArr.Data.DataType is TimestampType tsType)
                {
                    string? arrowTz = tsType.Timezone;
                    if (!string.IsNullOrEmpty(arrowTz))
                    {
                        try 
                        {
                            tzi = TimeZoneInfo.FindSystemTimeZoneById(arrowTz);
                        }
                        catch 
                        {
                            Console.WriteLine("No TimeZone Found.");
                        }
                    }
                }

                // 2. return TimeZone reader
                return baseAccessor = idx => 
                {
                    return array.GetDateTimeOffsetOptimized(idx, tzi);
                };
                }
                else if (underlyingType == typeof(DateOnly))
                {
                    baseAccessor = idx => array.GetDateOnly(idx);
                }

                else if (underlyingType == typeof(TimeOnly))
                {
                    baseAccessor = idx => array.GetTimeOnly(idx);
                }
                else if (underlyingType == typeof(TimeSpan))
                {
                    baseAccessor = idx => 
                    {
                        TimeSpan? v = array.GetTimeSpan(idx);
                        if (!v.HasValue) return null;
                        return v.Value;
                    };
                }
            }

            // ---------------------------------------------------------
            // F# Option Wrap
            // ---------------------------------------------------------
            
            if (baseAccessor == null) return _ => null;

            // For F# Option，null -> None，value -> Some(value)
            if (isFSharpOption)
            {
                var wrapper = FSharpHelper.CreateOptionWrapper(targetType);
                return idx => wrapper(baseAccessor(idx));
            }

            return baseAccessor;
        }
        // --- Helper: Check whether is scalartype ---
        private static bool IsScalarType(Type t)
        {
            var underlying = Nullable.GetUnderlyingType(t) ?? t;

            return underlying.IsPrimitive 
                || underlying == typeof(string)
                || underlying == typeof(decimal)
                || underlying == typeof(DateTime)
                || underlying == typeof(DateOnly)
                || underlying == typeof(TimeOnly)
                || underlying == typeof(TimeSpan)
                || underlying == typeof(DateTimeOffset)
                || FSharpHelper.IsFSharpOption(t); 
        }
        /// <summary>
        /// Create a high-performance accessor for a single Arrow Array.
        /// Used by Series.AsSeq().
        /// </summary>
        public static Func<int, object?> GetSeriesAccessor<T>(IArrowArray array)
            => CreateAccessor(array, typeof(T));
        public static T[] ReadColumn<T>(IArrowArray array)
        {
            var accessor = CreateAccessor(array, typeof(T));
            int len = array.Length;
            var result = new T[len];
            
            for (int i = 0; i < len; i++)
            {
                var val = accessor(i);
                result[i] = val == null ? default! : (T)val;
            }
            return result;
        }
        /// <summary>
        /// Read single Array index i item
        /// </summary>
        public static T? ReadItem<T>(IArrowArray array, int index)
        {
            var accessor = CreateAccessor(array, typeof(T));
            var val = accessor(index);
            return val == null ? default : (T)val;
        }
    }
    
}