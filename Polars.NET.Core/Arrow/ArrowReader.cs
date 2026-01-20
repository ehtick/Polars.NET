using System.Collections;
using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Arrays;
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
            // 1. Dynamic Fallback (for object)
            if (targetType == typeof(object))
            {
                targetType = ArrowTypeResolver.GetNetTypeFromArrowType(array.Data.DataType);
            }

            // 2. Handle F# Option Wrapper
            bool isFSharpOption = FSharpHelper.IsFSharpOption(targetType);
            Type underlyingType = isFSharpOption 
                ? FSharpHelper.GetUnderlyingType(targetType) 
                : (Nullable.GetUnderlyingType(targetType) ?? targetType);

            // 3. Create Core Accessor
            Func<int, object?> accessor = CreateCoreAccessor(array, underlyingType);

            // 4. Wrap if F# Option
            if (isFSharpOption)
            {
                var wrapper = FSharpHelper.CreateOptionWrapper(targetType);
                return idx => wrapper(accessor(idx));
            }

            return accessor;
        }
        private static Func<int, object?> CreateCoreAccessor(IArrowArray array, Type type)
        {
            if (array is StructArray sa) return CreateStructAccessor(sa, type);
            
            if (array is ListArray || array is LargeListArray || array is FixedSizeListArray)
                return CreateListAccessor(array, type);

            return CreatePrimitiveAccessor(array, type);
        }

        // =============================================================
        // 1. Primitive Accessor (Optimized)
        // =============================================================
        private static Func<int, object?> CreatePrimitiveAccessor(IArrowArray array, Type type)
        {
            // String
            if (type == typeof(string)) return array.GetStringValue;

            // Boolean
            if (type == typeof(bool)) return idx => ((BooleanArray)array).GetValue(idx);

            // Integers (Unrolled for Performance)
            if (type == typeof(int))    return idx => (int?)array.GetInt64Value(idx);
            if (type == typeof(long))   return idx => array.GetInt64Value(idx);
            if (type == typeof(short))  return idx => (short?)array.GetInt64Value(idx);
            if (type == typeof(byte))   return idx => (byte?)array.GetInt64Value(idx);
            if (type == typeof(sbyte))  return idx => (sbyte?)array.GetInt64Value(idx);
            if (type == typeof(uint))   return idx => (uint?)array.GetInt64Value(idx);
            if (type == typeof(ulong))  return idx => (ulong?)array.GetInt64Value(idx);
            if (type == typeof(ushort)) return idx => (ushort?)array.GetInt64Value(idx);

            // Floats
            if (type == typeof(double)) return idx => array.GetDoubleValue(idx);
            if (type == typeof(float))  return idx => (float?)array.GetDoubleValue(idx);
            if (type == typeof(Half))   return idx => (Half?)array.GetDoubleValue(idx);

            // Decimal
            if (type == typeof(decimal))
            {
                if (array is Decimal128Array decArr) return idx => decArr.GetValue(idx); // Fast path
                if (array is DoubleArray dArr) return idx => (decimal?)dArr.GetValue(idx);
                return _ => null;
            }

            // Dates & Times
            if (type == typeof(DateTime))
            {
                if (array is TimestampArray tsArr) return idx => tsArr.GetTimestamp(idx)?.DateTime;
                
                if (array is Date64Array d64Arr) return idx => d64Arr.GetDateTime(idx);
                
                if (array is Date32Array d32Arr) return idx => d32Arr.GetDateTime(idx);

                if (array is Int64Array i64Arr) 
                {
                     return idx => 
                     {
                         long? v = i64Arr.GetValue(idx);
                         return v.HasValue ? DateTime.UnixEpoch.AddTicks(v.Value * 10) : null;
                     };
                }

                return _ => throw new NotSupportedException($"Cannot read DateTime from Arrow Array type: {array.GetType().Name}");;
            }
            
            if (type == typeof(DateOnly)) return idx => array.GetDateOnly(idx);
            
            if (type == typeof(TimeOnly)) return idx => array.GetTimeOnly(idx);
            
            if (type == typeof(TimeSpan)) 
            {
                return idx => array.GetTimeSpan(idx);
            }

            if (type == typeof(DateTimeOffset))
            {
                // Prepare TimeZone Info ONCE, not per row
                TimeZoneInfo? tzi = null;
                if (array is TimestampArray tsArr && tsArr.Data.DataType is TimestampType tsType && !string.IsNullOrEmpty(tsType.Timezone))
                {
                    try { tzi = TimeZoneInfo.FindSystemTimeZoneById(tsType.Timezone); } catch { }
                }
                return idx => array.GetDateTimeOffsetOptimized(idx, tzi);
            }

            // Binary
            if (type == typeof(byte[]))
            {
                 if (array is BinaryArray ba) return idx => ba.GetBytes(idx).ToArray();
                 if (array is LargeBinaryArray lba) return idx => lba.GetBytes(idx).ToArray();
                 if (array is FixedSizeBinaryArray fba) return idx => fba.GetBytes(idx).ToArray();
            }

            return _ => null;
        }
        // =============================================================
        // 2. Struct Accessor
        // =============================================================
        private static Func<int, object?> CreateStructAccessor(StructArray structArray, Type type)
        {
            if (type == typeof(Dictionary<string, object>) || type == typeof(object))
            {
                var fieldAccessors = new Dictionary<string, Func<int, object?>>();
                var fields = ((StructType)structArray.Data.DataType).Fields;
                
                for (int i = 0; i < fields.Count; i++)
                {
                    var fieldName = fields[i].Name;
                    var childArray = structArray.Fields[i];
                    fieldAccessors[fieldName] = CreateAccessor(childArray, typeof(object));
                }

                return idx =>
                {
                    if (structArray.IsNull(idx)) return null;
                    var dict = new Dictionary<string, object?>(fieldAccessors.Count);
                    foreach (var kv in fieldAccessors)
                    {
                        dict[kv.Key] = kv.Value(idx);
                    }
                    return dict;
                };
            }

            // POCO 
            var props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                            .Where(p => p.CanWrite).ToArray();
            
            var structType = (StructType)structArray.Data.DataType;
            var setters = new List<Action<object, int>>();

            foreach (var prop in props)
            {
                // Find Field Index
                int fieldIndex = -1;
                for (int k = 0; k < structType.Fields.Count; k++)
                {
                    if (string.Equals(structType.Fields[k].Name, prop.Name, StringComparison.OrdinalIgnoreCase)) 
                    { 
                        fieldIndex = k; break; 
                    }
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

            return idx => 
            {
                if (structArray.IsNull(idx)) return null;
                var instance = Activator.CreateInstance(type)!;
                foreach (var setter in setters) setter(instance, idx);
                return instance;
            };
        }

        // =============================================================
        // 3. List Accessor
        // =============================================================
        private static Func<int, object?> CreateListAccessor(IArrowArray array, Type type)
        {
            // Determine Element Type
            Type elementType = typeof(object);
            if (type.IsGenericType) elementType = type.GetGenericArguments()[0];
            else if (type.IsArray) elementType = type.GetElementType()!;
            
            bool isFSharpList = FSharpHelper.IsFSharpList(type);

            // Normalize Array Access
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
                getOffset = i => (long)(i + array.Offset) * width; // Correct Offset logic
                isNull = fixedArr.IsNull;
            }

            var childGetter = CreateAccessor(valuesArray, elementType);

            return idx =>
            {
                if (isNull(idx)) return null;

                long start = getOffset(idx);
                long end = getOffset(idx + 1);
                int count = (int)(end - start);

                // Create List<Element>
                var listType = typeof(List<>).MakeGenericType(elementType);
                var list = (IList)Activator.CreateInstance(listType, count)!;

                for (int k = 0; k < count; k++)
                {
                    var val = childGetter((int)(start + k));
                    list.Add(val);
                }

                if (type.IsArray)
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