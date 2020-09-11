using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using JDBC.NET.Proto;

namespace JDBC.NET.Data
{
    public class JdbcDataReader : DbDataReader
    {
        #region Fields
        private bool _isClosed;
        private bool _isDisposed;
        private DataTable _schemaTable;
        private readonly IEnumerator<List<dynamic>> _enumerator;
        #endregion

        #region Properties
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
        internal JdbcDataReader(ExecuteStatementResponse response)
        {
            Response = response;
            //_enumerator = response.Data.GetEnumerator();
        }
        #endregion

        #region Public Methods
        public override object GetValue(int ordinal)
        {
            return null;

            /*
            CheckOpen();

            if (_enumerator.Current == null)
                throw new ArgumentNullException();

            var value = _enumerator.Current[ordinal];

            if (value == null)
                return DBNull.Value;

            var fieldType = GetFieldType(ordinal);

            if (value.GetType() != fieldType)
            {
                try
                {
                    return Convert.ChangeType(value, fieldType);
                }
                catch (InvalidCastException)
                {
                    return value;
                }
            }

            return value;
            */
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
                _schemaTable.Columns.Add(SchemaTableColumn.ColumnOrdinal, typeof(int));
                _schemaTable.Columns.Add(SchemaTableOptionalColumn.IsReadOnly, typeof(bool));
                _schemaTable.Columns.Add(SchemaTableColumn.DataType, typeof(Type));
                _schemaTable.Columns.Add("DataTypeName", typeof(string));

                foreach (var column in Response.Columns)
                {
                    _schemaTable.Rows.Add(
                        column.Name,
                        column.Ordinal,
                        true,
                        typeof(object), // TODO : Need to implement type conversion
                        column.Type
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
                if (Response.Columns[i].Name == name)
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
            _schemaTable = null;
            _isClosed = true;
        }
        #endregion

        #region IDisposable
        protected override void Dispose(bool disposing)
        {
            if (_isDisposed)
                return;

            if (disposing)
                Close();

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
