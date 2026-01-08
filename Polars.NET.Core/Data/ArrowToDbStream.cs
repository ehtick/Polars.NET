using System.Collections;
using System.Data;
using System.Data.Common;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace Polars.NET.Core.Data
{
    /// <summary>
    /// Convert Arrow RecordBatch Stream to IDataReader。
    /// For Polars/Arrow Sink to Database streamingly (With SqlBulkCopy, etc.)
    /// </summary>
    public class ArrowToDbStream : DbDataReader
    {
        private readonly IEnumerator<RecordBatch> _batchEnumerator;
        private Schema? _schema; 
        private RecordBatch? _currentBatch;
        private int _currentRowIndex;
        private bool _isClosed;
        private readonly Dictionary<string, Type>? _typeOverrides;

        public ArrowToDbStream(IEnumerable<RecordBatch> stream,Dictionary<string, Type>? typeOverrides = null)
        {
            ArgumentNullException.ThrowIfNull(stream);
            _batchEnumerator = stream.GetEnumerator();
            _currentRowIndex = -1;
            _typeOverrides = typeOverrides ?? new Dictionary<string, Type>(StringComparer.OrdinalIgnoreCase);
        }

        private bool EnsureSchema()
        {
            if (_schema != null) return true;
            
            if (_batchEnumerator.MoveNext())
            {
                _currentBatch = _batchEnumerator.Current;
                _schema = _currentBatch.Schema;
                return true;
            }
            
            _isClosed = true;
            return false;
        }

        public override bool Read()
        {
            if (_isClosed) return false;

            if (_schema == null)
            {
                if (!EnsureSchema()) return false; // null stream
            }

            _currentRowIndex++;

            // Current Batch is not null
            if (_currentBatch != null && _currentRowIndex < _currentBatch.Length)
            {
                return true;
            }

            // Need next Batch
            if (_batchEnumerator.MoveNext())
            {
                _currentBatch?.Dispose();
                _currentBatch = _batchEnumerator.Current;
                _schema = _currentBatch.Schema; 
                _currentRowIndex = 0;

                if (_currentBatch.Length == 0) return Read();

                return true;
            }

            // No more data batch
            _currentBatch = null!;
            _isClosed = true;
            return false;
        }

        public override object GetValue(int ordinal)
        {
            // Get raw value
            if (_currentBatch == null) throw new InvalidOperationException("No batch");
            var col = _currentBatch.Column(ordinal);
            if (col.IsNull(_currentRowIndex)) return DBNull.Value;
            
            var val = GetValueFromArray(col, _currentRowIndex);
            
            // Get target dtype
            var targetType = GetFieldType(ordinal);

            if (val.GetType() == targetType) return val;

            // -------------------------------------------------------------
            // Magic Zone: Physical type -> Logical type
            // -------------------------------------------------------------

            // A. Long (Int64) degeneration -> Timestamp, Time, Duration
            if (val is long lVal)
            {
                // 1. long -> DateTime (Timestamp)
                if (targetType == typeof(DateTime))
                {
                    // Microseconds (Polars default)
                    return DateTime.UnixEpoch.AddTicks(lVal * 10); 
                }

                // 2. long -> DateTimeOffset
                if (targetType == typeof(DateTimeOffset))
                {
                    return new DateTimeOffset(DateTime.UnixEpoch.Ticks + (lVal * 10), TimeSpan.Zero);
                }

                // 3. long -> TimeSpan (Duration)
                if (targetType == typeof(TimeSpan))
                {
                    // 1 us = 10 ticks
                    return new TimeSpan(lVal * 10);
                }

                // 4. long -> TimeOnly (Time64)
                if (targetType == typeof(TimeOnly))
                {
                    return new TimeOnly(lVal * 10);
                }
            }

            // B. Int (Int32) degeneration -> Date32
            else if (val is int iVal)
            {
                // int -> DateOnly (Date32)
                if (targetType == typeof(DateOnly))
                {
                    return DateOnly.FromDateTime(DateTime.UnixEpoch.AddDays(iVal));
                }
            }

            // C. Otheres (float -> decimal if needed)
            
            return val;
        }
        private static object GetValueFromArray(IArrowArray array, int index)
        {
            switch (array)
            {
                case Int32Array arr: return arr.GetValue(index) ?? (object)DBNull.Value;
                case Int64Array arr: return arr.GetValue(index) ?? (object)DBNull.Value;
                case FloatArray arr: return arr.GetValue(index) ?? (object)DBNull.Value;
                case DoubleArray arr: return arr.GetValue(index) ?? (object)DBNull.Value;
                case BooleanArray arr: return arr.GetValue(index) ?? (object)DBNull.Value;
                case StringArray arr: return arr.GetString(index);
                case LargeStringArray arr: return arr.GetString(index);
                case StringViewArray arr: return arr.GetString(index);
                case BinaryArray arr: return arr.GetBytes(index).ToArray();
                case BinaryViewArray arr: return arr.GetBytes(index).ToArray();
                case FixedSizeBinaryArray arr: return arr.GetBytes(index).ToArray();
                case FixedSizeListArray arr:
                    return FormatFixedSizeList(arr, index);
                case Date32Array arr:
                    var dt = arr.GetDateTime(index);
                    return dt.HasValue ? DateOnly.FromDateTime(dt.Value) : DBNull.Value;

                case TimestampArray arr: 
                    return arr.GetTimestamp(index)?.DateTime ?? (object)DBNull.Value;
                case Date64Array arr:
                     return arr.GetDateTime(index) ?? (object)DBNull.Value;                
                case Time64Array arr:
                    var micros = arr.GetMicroSeconds(index);
                    if (!micros.HasValue) return DBNull.Value;
                    return TimeOnly.FromTimeSpan(TimeSpan.FromTicks(micros.Value * 10));
                case DurationArray arr:
                {
                    var val = arr.GetValue(index); 
                    if (!val.HasValue) return DBNull.Value;

                    return new TimeSpan(val.Value * 10);
                }

                case ListArray arr:
                    return FormatList(arr, index);
                case StructArray arr: return FormatStruct(arr, index);
                
                default:
                    throw new NotSupportedException($"ArrowToDbStream does not yet support type: {array.GetType().Name}");
            }
        }
        // -------------------------------------------------------------
        // JSON Format Helper
        // -------------------------------------------------------------
        private static string FormatJsonValue(object? val)
        {
            if (val == null || val == DBNull.Value) return "null";

            return val switch
            {
                string jsonStr when jsonStr.Length > 1 && (jsonStr.StartsWith("{") || jsonStr.StartsWith("[")) 
                    => jsonStr,

                string s => System.Text.Json.JsonSerializer.Serialize(s),
                char c => System.Text.Json.JsonSerializer.Serialize(c.ToString()),

                // Primitive Type
                bool b => b ? "true" : "false",
                
                // DateTime (Keep ISO 8601 format)
                DateTime dt => $"\"{dt:O}\"",
                DateTimeOffset dto => $"\"{dto:O}\"",
                DateOnly d => $"\"{d:yyyy-MM-dd}\"",
                TimeOnly t => $"\"{t:HH:mm:ss.ffffff}\"",
                TimeSpan ts => $"\"{ts}\"", 

                // others.ToString
                _ => val.ToString()!
            };
        }
        // Format Struct -> JSON Object
        private static string FormatStruct(StructArray arr, int index)
        {
            if (arr.IsNull(index)) return "null";

            var structType = (StructType)arr.Data.DataType;
            var sb = new StringBuilder("{");
            
            for (int i = 0; i < structType.Fields.Count; i++)
            {
                if (i > 0) sb.Append(", ");
                
                var field = structType.Fields[i];
                var childArr = arr.Fields[i];
                
                sb.Append($"\"{field.Name}\": ");
                
                object val = GetValueFromArray(childArr, index);
                sb.Append(FormatJsonValue(val));
            }
            sb.Append("}");
            return sb.ToString();
        }
        // Format Array
        private static string FormatFixedSizeList(FixedSizeListArray arr, int index)
        {
            if (arr.IsNull(index)) return "null";

            var type = (FixedSizeListType)arr.Data.DataType;
            int width = type.ListSize;
            int start = index * width;
            
            var sb = new StringBuilder("[");
            for (int i = 0; i < width; i++)
            {
                if (i > 0) sb.Append(", ");
                object val = GetValueFromArray(arr.Values, start + i);
                sb.Append(FormatJsonValue(val));
            }
            sb.Append("]");
            return sb.ToString();
        }
        // Format List
        private static string FormatList(ListArray arr, int index)
        {
            if (arr.IsNull(index)) return "null";

            int start = arr.ValueOffsets[index];
            int end = arr.ValueOffsets[index + 1];
            int count = end - start;

            var sb = new StringBuilder("[");
            for (int i = 0; i < count; i++)
            {
                if (i > 0) sb.Append(", ");
                object val = GetValueFromArray(arr.Values, start + i);
                sb.Append(FormatJsonValue(val));
            }
            sb.Append("]");
            return sb.ToString();
        }
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        {
            if (_currentBatch == null) return 0;
            var column = _currentBatch.Column(ordinal);
            
            if (column.IsNull(_currentRowIndex)) return 0;

            ReadOnlySpan<byte> bytes;
            
            if (column is BinaryArray binArr)
                bytes = binArr.GetBytes(_currentRowIndex);
            // else if (column is LargeBinaryArray largeBinArr) ...
            else
                throw new InvalidCastException($"Column {ordinal} is not a BinaryArray.");

            if (buffer == null)
            {
                return bytes.Length;
            }

            int available = bytes.Length - (int)dataOffset;
            int toCopy = Math.Min(available, length);

            if (toCopy > 0)
            {
                bytes.Slice((int)dataOffset, toCopy).CopyTo(buffer.AsSpan(bufferOffset));
            }

            return toCopy;
        }

        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        {
            if (_currentBatch == null) return 0;
            
            string val = GetString(ordinal);
            
            if (val == null) return 0;

            if (buffer == null)
            {
                return val.Length;
            }

            int available = val.Length - (int)dataOffset;
            int toCopy = Math.Min(available, length);

            if (toCopy > 0)
            {
                val.CopyTo((int)dataOffset, buffer, bufferOffset, toCopy);
            }

            return toCopy;
        }
        // --- Schema Interface ---

        public override int FieldCount 
        { 
            get 
            {
                EnsureSchema();
                return _schema?.FieldsList.Count ?? 0;
            }
        }

        public override string GetName(int ordinal)
        {
            EnsureSchema();
            return _schema?.GetFieldByIndex(ordinal).Name ?? string.Empty;
        }

        public override int GetOrdinal(string name)
        {
            EnsureSchema();
            return _schema?.GetFieldIndex(name) ?? -1;
        }

        public override Type GetFieldType(int ordinal)
        {
            EnsureSchema();
            if (_schema == null) return typeof(object);
            var field = _schema.GetFieldByIndex(ordinal);

            if (_typeOverrides!.TryGetValue(field.Name, out var targetType))
            {   
                return targetType;
            }
            return field.DataType switch
            {
                Int32Type => typeof(int),
                Int64Type => typeof(long),
                FloatType => typeof(float),
                DoubleType => typeof(double),
                BooleanType => typeof(bool),
                StringType => typeof(string),
                LargeStringType => typeof(string),
                StringViewType => typeof(string), 
                TimestampType => typeof(DateTime),
                Date32Type => typeof(DateOnly),
                Date64Type => typeof(DateTime),
                Time32Type => typeof(TimeOnly),
                Time64Type => typeof(TimeOnly),
                DurationType => typeof(TimeSpan),
                BinaryType => typeof(byte[]),
                _ => typeof(object)
            };
        }

        public override bool IsDBNull(int ordinal)
        {
            if (_currentBatch == null) return true;
            return _currentBatch.Column(ordinal).IsNull(_currentRowIndex);
        }

        // --- Boilerplate ---
        public override object this[int ordinal] => GetValue(ordinal);
        public override object this[string name] => GetValue(GetOrdinal(name));
        public override bool HasRows => EnsureSchema();
        public override bool NextResult() => false; 
        public override int Depth => 0;
        public override int RecordsAffected => -1;

        public override bool IsClosed => _isClosed;

        public override void Close()
        {
            if (!_isClosed)
            {
                _isClosed = true;
                _batchEnumerator.Dispose();
                _currentBatch = null;
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                Close();
            }
            base.Dispose(disposing);
        }
        public override int GetValues(object[] values)
        {
            if (_isClosed || _currentBatch == null) return 0;

            int copyCount = Math.Min(values.Length, FieldCount);

            for (int i = 0; i < copyCount; i++)
            {
                values[i] = GetValue(i);
            }

            return copyCount;
        }
        public override IEnumerator GetEnumerator() => new DbEnumerator(this, closeReader: false);
        public override string GetDataTypeName(int ordinal) => GetFieldType(ordinal).Name;
        
        public override bool GetBoolean(int ordinal) => (bool)GetValue(ordinal);
        public override byte GetByte(int ordinal) => (byte)GetValue(ordinal);
        public override char GetChar(int ordinal) => (char)GetValue(ordinal);
        public override DateTime GetDateTime(int ordinal) => (DateTime)GetValue(ordinal);
        public override decimal GetDecimal(int ordinal) => Convert.ToDecimal(GetValue(ordinal));
        public override double GetDouble(int ordinal) => (double)GetValue(ordinal);
        public override float GetFloat(int ordinal) => (float)GetValue(ordinal);
        public override Guid GetGuid(int ordinal) => (Guid)GetValue(ordinal);
        public override short GetInt16(int ordinal) => (short)GetValue(ordinal);
        public override int GetInt32(int ordinal) => (int)GetValue(ordinal);
        public override long GetInt64(int ordinal) => (long)GetValue(ordinal);
        public override string GetString(int ordinal) => (string)GetValue(ordinal);

        public override DataTable GetSchemaTable()
        {
            EnsureSchema();
            if (_schema == null) return null!;

            var table = new DataTable("SchemaTable");

            // 1. Define Schema Table Column
            // Ref: https://learn.microsoft.com/en-us/dotnet/api/system.data.idatareader.getschematable
            table.Columns.Add("ColumnName", typeof(string));
            table.Columns.Add("ColumnOrdinal", typeof(int));
            table.Columns.Add("ColumnSize", typeof(int));
            table.Columns.Add("NumericPrecision", typeof(short));
            table.Columns.Add("NumericScale", typeof(short));
            table.Columns.Add("DataType", typeof(Type));
            table.Columns.Add("ProviderType", typeof(Type));
            table.Columns.Add("IsLong", typeof(bool));
            table.Columns.Add("AllowDBNull", typeof(bool));
            table.Columns.Add("IsReadOnly", typeof(bool));
            table.Columns.Add("IsRowVersion", typeof(bool));
            table.Columns.Add("IsUnique", typeof(bool));
            table.Columns.Add("IsKey", typeof(bool));
            table.Columns.Add("IsAutoIncrement", typeof(bool));
            table.Columns.Add("BaseSchemaName", typeof(string));
            table.Columns.Add("BaseCatalogName", typeof(string));
            table.Columns.Add("BaseTableName", typeof(string));
            table.Columns.Add("BaseColumnName", typeof(string));

            // 2. Fill Data
            for (int i = 0; i < _schema.FieldsList.Count; i++)
            {
                var field = _schema.GetFieldByIndex(i);
                var row = table.NewRow();

                row["ColumnName"] = field.Name;
                row["ColumnOrdinal"] = i;
                row["DataType"] = GetFieldType(i); 
                row["ColumnSize"] = -1; 
                row["AllowDBNull"] = field.IsNullable;
                
                // Default
                row["NumericPrecision"] = DBNull.Value;
                row["NumericScale"] = DBNull.Value;
                row["IsLong"] = false;
                row["IsReadOnly"] = true; 
                row["IsRowVersion"] = false;
                row["IsUnique"] = false;
                row["IsKey"] = false;
                row["IsAutoIncrement"] = false;
                row["BaseSchemaName"] = DBNull.Value;
                row["BaseCatalogName"] = DBNull.Value;
                row["BaseTableName"] = DBNull.Value;
                row["BaseColumnName"] = field.Name;

                table.Rows.Add(row);
            }
            return table;
        }
    }
}