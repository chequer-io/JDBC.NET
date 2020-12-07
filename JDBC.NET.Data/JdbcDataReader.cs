using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using JDBC.NET.Data.Converters;
using JDBC.NET.Data.Models;
using JDBC.NET.Proto;

namespace JDBC.NET.Data
{
    public class JdbcDataReader : DbDataReader
    {
        #region Fields
        private bool _isClosed;
        private bool _isDisposed;
        private DataTable _schemaTable;
        private readonly JdbcDataEnumerator _enumerator;
        #endregion

        #region Properties
        private JdbcCommand Command { get; }

        private ExecuteStatementResponse Response { get; }

        public override int FieldCount => Response.Columns.Count;

        public override object this[int ordinal]
        {
            get
            {
                CheckOpen();
                return GetValue(ordinal);
            }
        }

        public override object this[string name]
        {
            get
            {
                CheckOpen();
                return GetValue(GetOrdinal(name));
            }
        }

        public override bool HasRows => Response.HasRows;

        public override bool IsClosed => _isClosed;

        public override int RecordsAffected => Response.RecordsAffected;

        public override int Depth => 0;
        #endregion

        #region Constructor
        internal JdbcDataReader(JdbcCommand command, ExecuteStatementResponse response)
        {
            Command = command;
            Response = response;

            if (!(Command.Connection is JdbcConnection jdbcConnection))
                throw new InvalidOperationException();

            _enumerator = new JdbcDataEnumerator(jdbcConnection, response);
        }
        #endregion

        #region Public Methods
        public override object GetValue(int ordinal)
        {
            CheckOpen();

            if (_enumerator.Current == null)
                throw new ArgumentNullException();

            var item = _enumerator.Current.Items[ordinal];

            if (item.IsNull)
                return DBNull.Value;

            if (item.ValueCase == JdbcDataItem.ValueOneofCase.ByteArray)
                return item.ByteArray.ToByteArray();

            var textValue = item.Text;

            try
            {
                var fieldType = GetFieldType(ordinal);

                return fieldType != null
                    ? Convert.ChangeType(textValue, fieldType)
                    : textValue;
            }
            catch (InvalidCastException)
            {
                return textValue;
            }
        }

        public override int GetValues(object[] values)
        {
            for (var i = 0; i < FieldCount; i++)
                values[i] = GetValue(i);

            return values.Length;
        }

        public override DataTable GetSchemaTable()
        {
            CheckOpen();

            if (_schemaTable == null)
            {
                _schemaTable = new DataTable();
                _schemaTable.Columns.Add(SchemaTableColumn.ColumnName, typeof(string));
                _schemaTable.Columns.Add(SchemaTableColumn.ColumnSize, typeof(int));
                _schemaTable.Columns.Add(SchemaTableColumn.ColumnOrdinal, typeof(int));
                _schemaTable.Columns.Add(SchemaTableColumn.NumericPrecision, typeof(int));
                _schemaTable.Columns.Add(SchemaTableColumn.NumericScale, typeof(int));
                _schemaTable.Columns.Add(SchemaTableColumn.BaseTableName, typeof(string));
                _schemaTable.Columns.Add(SchemaTableColumn.BaseSchemaName, typeof(string));
                _schemaTable.Columns.Add(SchemaTableOptionalColumn.IsAutoIncrement, typeof(bool));
                _schemaTable.Columns.Add("IsCaseSensitive", typeof(bool));
                _schemaTable.Columns.Add("IsDefinitelyWritable", typeof(bool));
                _schemaTable.Columns.Add("IsSearchable", typeof(bool));
                _schemaTable.Columns.Add(SchemaTableColumn.AllowDBNull, typeof(bool));
                _schemaTable.Columns.Add("IsWritable", typeof(bool));
                _schemaTable.Columns.Add("IsCurrency", typeof(bool));
                _schemaTable.Columns.Add(SchemaTableOptionalColumn.IsReadOnly, typeof(bool));
                _schemaTable.Columns.Add(SchemaTableOptionalColumn.IsHidden, typeof(bool));
                _schemaTable.Columns.Add("IsSigned", typeof(bool));
                _schemaTable.Columns.Add(SchemaTableColumn.DataType, typeof(Type));
                _schemaTable.Columns.Add("DataTypeClassName", typeof(string));
                _schemaTable.Columns.Add("DataTypeName", typeof(string));
                _schemaTable.Columns.Add(SchemaTableColumn.IsKey, typeof(bool));

                foreach (var column in Response.Columns)
                {
                    _schemaTable.Rows.Add(
                        column.ColumnName,
                        column.ColumnDisplaySize,
                        column.Ordinal,
                        column.ColumnPrecision,
                        column.ColumnScale,
                        column.TableName,
                        column.SchemaName,
                        column.IsAutoIncrement,
                        column.IsCaseSensitive,
                        column.IsDefinitelyWritable,
                        column.IsSearchable,
                        column.IsNullable,
                        column.IsWritable,
                        column.IsCurrency,
                        column.IsReadOnly,
                        false,
                        column.IsSigned,
                        JdbcTypeConverter.ToType((JdbcDataTypeCode)column.DataTypeCode),
                        column.DataTypeClassName,
                        column.DataTypeName,
                        false
                    );
                }
            }

            return _schemaTable;
        }

        public override string GetName(int ordinal)
        {
            return GetSchemaTable()?.Rows[ordinal][SchemaTableColumn.ColumnName].ToString();
        }

        public override int GetOrdinal(string name)
        {
            CheckOpen();

            for (var i = 0; i < FieldCount; i++)
                if (Response.Columns[i].ColumnName == name)
                    return i;

            return -1;
        }

        public override string GetDataTypeName(int ordinal)
        {
            return (string)GetSchemaTable()?.Rows[ordinal]["DataTypeName"];
        }

        public override Type GetFieldType(int ordinal)
        {
            return (Type)GetSchemaTable()?.Rows[ordinal][SchemaTableColumn.DataType];
        }

        public override short GetInt16(int ordinal)
        {
            return (short)GetValue(ordinal);
        }

        public override int GetInt32(int ordinal)
        {
            return (int)GetValue(ordinal);
        }

        public override long GetInt64(int ordinal)
        {
            return (long)GetValue(ordinal);
        }

        public override float GetFloat(int ordinal)
        {
            return (float)GetValue(ordinal);
        }

        public override double GetDouble(int ordinal)
        {
            return (double)GetValue(ordinal);
        }

        public override decimal GetDecimal(int ordinal)
        {
            return (decimal)GetValue(ordinal);
        }

        public override bool GetBoolean(int ordinal)
        {
            return (bool)GetValue(ordinal);
        }

        public override string GetString(int ordinal)
        {
            return GetValue(ordinal).ToString();
        }

        public override char GetChar(int ordinal)
        {
            throw new NotSupportedException();
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            throw new NotSupportedException();
        }

        public override byte GetByte(int ordinal)
        {
            throw new NotSupportedException();
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            throw new NotSupportedException();
        }

        public override DateTime GetDateTime(int ordinal)
        {
            return (DateTime)GetValue(ordinal);
        }

        public override Guid GetGuid(int ordinal)
        {
            throw new NotSupportedException();
        }

        public override bool IsDBNull(int ordinal)
        {
            return GetValue(ordinal) is DBNull;
        }
        #endregion

        #region Private Methods
        private void CheckOpen()
        {
            CheckDispose();

            if (_isClosed)
                throw new InvalidOperationException("DataReader is not open.");
        }

        private void CheckDispose()
        {
            if (_isDisposed)
                throw new ObjectDisposedException(ToString());
        }
        #endregion

        #region IDataReader
        public override bool Read()
        {
            CheckOpen();
            return _enumerator.MoveNext();
        }

        public override bool NextResult()
        {
            CheckOpen();
            return false;
        }

        public override void Close()
        {
            _enumerator?.Dispose();

            if (!(Command.Connection is JdbcConnection jdbcConnection))
                throw new InvalidOperationException();

            if (!string.IsNullOrEmpty(Response.ResultSetId))
            {
                jdbcConnection.Bridge.Reader.closeResultSet(new CloseResultSetRequest
                {
                    ResultSetId = Response.ResultSetId
                });
            }

            _schemaTable = null;
            _isClosed = true;
        }
        #endregion

        #region IDisposable
        protected override void Dispose(bool disposing)
        {
            if (_isDisposed)
                return;

            _isDisposed = true;
            base.Dispose(disposing);
        }
        #endregion

        #region IEnumerable
        public override IEnumerator GetEnumerator()
        {
            CheckOpen();
            return _enumerator;
        }
        #endregion
    }
}
