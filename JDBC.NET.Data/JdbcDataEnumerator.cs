using System;
using System.Collections;
using System.Collections.Generic;
using Grpc.Core;
using JDBC.NET.Data.Models;
using JDBC.NET.Data.Utilities;
using JDBC.NET.Proto;

namespace JDBC.NET.Data
{
    public class JdbcDataEnumerator : IEnumerator<JdbcDataRow>
    {
        #region Fields
        private ReadResultSetResponse _currentResponse;
        private IEnumerator<JdbcDataRow> _currentChunk;
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
        }
        #endregion

        #region IEnumerator
        public JdbcDataRow Current { get; private set; }

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
                var rows = new List<JdbcDataRow>();

                while (spanReader.Length > spanReader.Position)
                {
                    var row = new JdbcDataRow();
                    var columnCount = spanReader.ReadInt32();

                    for (int i = 0; i < columnCount; i++)
                    {
                        var type = (JdbcItemType)spanReader.ReadByte();

                        if (type is JdbcItemType.Null)
                        {
                            row.Add(new JdbcDataItem
                            {
                                Type = type
                            });

                            continue;
                        }

                        var length = spanReader.ReadInt32();

                        row.Add(new JdbcDataItem
                        {
                            Value = spanReader.ReadBytes(length),
                            Type = type
                        });
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
