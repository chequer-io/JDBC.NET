using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Grpc.Core;
using JDBC.NET.Data.Converters;
using JDBC.NET.Data.Models;
using JDBC.NET.Proto;

namespace JDBC.NET.Data
{
    internal sealed class JdbcDataEnumerator : IEnumerator<object[]>
    {
        #region Fields
        private ReadResultSetResponse _currentResponse;
        private readonly JdbcDataChunk _chunk;
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

            Type[] fieldTypes = Response.Columns
                .Select(x => JdbcTypeConverter.ToType((JdbcDataTypeCode)x.DataTypeCode))
                .ToArray();

            _chunk = new JdbcDataChunk(fieldTypes);
        }
        #endregion

        #region IEnumerator
        public object[] Current => _chunk.Current;

        object IEnumerator.Current => _chunk.Current;

        public bool MoveNext()
        {
            while (true)
            {
                if (StreamingCall == null)
                    return false;

                if (_chunk.MoveNext())
                    return true;

                if (_currentResponse is { IsCompleted: true })
                    return false;

                if (!MoveNextChunk())
                    return false;

                _currentResponse = StreamingCall.ResponseStream.Current;
                _chunk.Update(_currentResponse.Rows.Memory);
            }
        }

        private bool MoveNextChunk()
        {
            try
            {
                var request = new ReadResultSetRequest
                {
                    ChunkSize = Connection.ConnectionStringBuilder.ChunkSize,
                    ResultSetId = Response.ResultSetId
                };

                StreamingCall.RequestStream.WriteAsync(request).Wait();
            }
            catch (AggregateException e) when (e.InnerExceptions.Count == 1)
            {
                throw e.InnerExceptions[0];
            }

            try
            {
                return StreamingCall.ResponseStream.MoveNext().Result;
            }
            catch (AggregateException e) when (e.InnerExceptions.Count == 1)
            {
                throw e.InnerExceptions[0];
            }
        }

        public void Reset()
        {
            throw new NotSupportedException();
        }
        #endregion

        #region IDisposable
        public void Dispose()
        {
            _chunk.Update(ReadOnlyMemory<byte>.Empty);
            StreamingCall?.RequestStream.CompleteAsync().Wait();
            StreamingCall?.Dispose();
        }
        #endregion
    }
}
