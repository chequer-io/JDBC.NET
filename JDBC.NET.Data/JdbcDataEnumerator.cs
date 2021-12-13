using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Grpc.Core;
using JDBC.NET.Data.Converters;
using JDBC.NET.Data.Models;
using JDBC.NET.Data.Utilities;
using JDBC.NET.Proto;

namespace JDBC.NET.Data
{
    public class JdbcDataEnumerator : IEnumerator<object[]>
    {
        #region Fields
        private ReadResultSetResponse _currentResponse;
        private IEnumerator<object[]> _currentChunk;
        private Type[] _fieldTypes;
        #endregion

        #region Properties
        private JdbcConnection Connection { get; }

        private JdbcResultSetResponse Response { get; }

        private AsyncDuplexStreamingCall<ReadResultSetRequest, ReadResultSetResponse> StreamingCall { get; }
        #endregion

        #region Constructor
        internal JdbcDataEnumerator(JdbcConnection connection, JdbcResultSetResponse response)
        {
            Connection = connection;
            Response = response;

            if (!string.IsNullOrEmpty(response.ResultSetId))
                StreamingCall = Connection.Bridge.Reader.readResultSet();

            _fieldTypes = Response.Columns
                .Select(x => JdbcTypeConverter.ToType((JdbcDataTypeCode)x.DataTypeCode))
                .ToArray();
        }
        #endregion

        #region IEnumerator
        public object[] Current { get; private set; }

        object IEnumerator.Current => Current;

        public bool MoveNext()
        {
            if (StreamingCall == null)
                return false;

            if (_currentChunk == null)
            {
                StreamingCall.RequestStream.WriteAsync(new ReadResultSetRequest
                {
                    ChunkSize = Connection.ConnectionStringBuilder.ChunkSize,
                    ResultSetId = Response.ResultSetId
                }).Wait();

                if (!StreamingCall.ResponseStream.MoveNext().Result)
                    return false;

                _currentResponse = StreamingCall.ResponseStream.Current;

                ReadOnlySpan<byte> currentRowsSpan = _currentResponse.Rows.Span;
                var spanReader = new UnsafeSpanReader(currentRowsSpan);
                var rows = new List<object[]>(Connection.ConnectionStringBuilder.ChunkSize);

                while (spanReader.Length > spanReader.Position)
                {
                    var row = new object[Response.Columns.Count];

                    for (int i = 0; i < row.Length; i++)
                    {
                        var type = (JdbcItemType)spanReader.ReadByte();

                        if (type is JdbcItemType.Null)
                        {
                            row[i] = DBNull.Value;
                        }
                        else
                        {
                            var length = spanReader.ReadInt32();
                            ReadOnlySpan<byte> valueSpan = spanReader.ReadSpan(length);

                            row[i] = ParseValue(i, type, valueSpan);
                        }
                    }

                    rows.Add(row);
                }

                _currentChunk = rows.GetEnumerator();
            }

            if (_currentChunk?.MoveNext() == false)
            {
                if (_currentResponse.IsCompleted)
                    return false;

                _currentChunk = null;
                return MoveNext();
            }

            Current = _currentChunk?.Current;
            return true;
        }

        private object ParseValue(int ordinal, JdbcItemType type, ReadOnlySpan<byte> value)
        {
            if (type is JdbcItemType.Binary)
                return value.ToArray();

            var textValue = Encoding.UTF8.GetString(value);
            var fieldType = _fieldTypes[ordinal];

            if (fieldType is null || fieldType == typeof(string))
                return textValue;

            return Convert.ChangeType(textValue, fieldType);
        }

        public void Reset()
        {
            throw new NotSupportedException();
        }
        #endregion

        #region IDisposable
        public void Dispose()
        {
            _currentChunk?.Dispose();
            StreamingCall?.RequestStream.CompleteAsync().Wait();
            StreamingCall?.Dispose();
        }
        #endregion
    }
}
