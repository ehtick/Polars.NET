using System.Collections;
using System.Data;
using System.Data.Common;
using System.Text.Json;
using Apache.Arrow;

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
        private Func<int, object?>[] _columnAccessors = System.Array.Empty<Func<int, object?>>();

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
            
            if (LoadNextBatch()) 
            {
                return true;
            }
            _isClosed = true;
            return false;
        }

        private bool LoadNextBatch()
        {
            if (_batchEnumerator.MoveNext())
            {
                // Release last batch
                _currentBatch?.Dispose();
                
                _currentBatch = _batchEnumerator.Current;
                
                // RebuildAccessors
                _schema = _currentBatch.Schema;
                RebuildAccessors(); 
                
                _currentRowIndex = -1;
                if (_currentBatch.Length == 0) return LoadNextBatch(); // 跳过空 Batch
                return true;
            }
            return false;
        }

        private void RebuildAccessors()
        {
            int count = _schema!.FieldsList.Count;
            _columnAccessors = new Func<int, object?>[count];

            for (int i = 0; i < count; i++)
            {
                var field = _schema.GetFieldByIndex(i);
                var arrowCol = _currentBatch!.Column(i);

                // Confirm target .NET type
                Type targetType;
                if (_typeOverrides != null && _typeOverrides.TryGetValue(field.Name, out var overrideType))
                {
                    targetType = overrideType;
                }
                else
                {
                    targetType = Arrow.ArrowTypeResolver.GetNetTypeFromArrowType(field.DataType);
                }

                _columnAccessors[i] = Arrow.ArrowReader.CreateAccessor(arrowCol, targetType);
            }
        }

        public override bool Read()
        {
            if (_isClosed) return false;

            _currentRowIndex++;

            if (_currentBatch != null && _currentRowIndex < _currentBatch.Length)
            {
                return true;
            }

            if (LoadNextBatch())
            {
                _currentRowIndex++;
                return true;
            }

            _currentBatch = null!;
            _isClosed = true;
            return false;
        }

        public override object GetValue(int ordinal)
        {
            // 1. GetRawValue
            if (_currentBatch == null) throw new InvalidOperationException("No batch");
            
            var val = _columnAccessors[ordinal](_currentRowIndex);
            
            if (val == null) return DBNull.Value;

            var targetType = GetFieldType(ordinal);

            // =========================================================
            // 🔥 Magic Zone for datetime
            // =========================================================

            if (val is DateTimeOffset dto && targetType == typeof(DateTime))
            {
                return dto.DateTime; 
            }

            if (val is long lVal)
            {
                 if (targetType == typeof(DateTime)) return DateTime.UnixEpoch.AddTicks(lVal * 10);
                 if (targetType == typeof(TimeSpan)) return new TimeSpan(lVal * 10);
                 if (targetType == typeof(TimeOnly)) return new TimeOnly(lVal * 10);
            }

            // =========================================================
            // 3. JSON 序列化 (针对对象、数组、结构体)
            // =========================================================
            if (val is not string && 
                !(val is IConvertible) && 
                !(val is byte[]) && 
                !(val is DateTime) && 
                !(val is DateTimeOffset) && 
                !(val is DateOnly) && 
                !(val is TimeOnly) && 
                !(val is TimeSpan))
            {
                return JsonSerializer.Serialize(val);
            }

            return val;
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
            var field = _schema!.GetFieldByIndex(ordinal);
            
            if (_typeOverrides != null && _typeOverrides.TryGetValue(field.Name, out var t)) return t;

            return Arrow.ArrowTypeResolver.GetNetTypeFromArrowType(field.DataType);
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
                _currentBatch?.Dispose();
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