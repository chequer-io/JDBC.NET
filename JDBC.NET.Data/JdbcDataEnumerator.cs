using System;
using System.Collections;
using System.Collections.Generic;
using Grpc.Core;
using JDBC.NET.Proto;

namespace JDBC.NET.Data
{
    public class JdbcDataEnumerator : IEnumerator<JdbcDataRow>
    {
        #region Properties
        private JdbcConnection Connection { get; }

        private ExecuteStatementResponse Response { get; }

        private AsyncDuplexStreamingCall<ReadResultSetRequest, JdbcDataRow> StreamingCall { get; }
        #endregion

        #region Constructor
        public JdbcDataEnumerator(JdbcConnection connection, ExecuteStatementResponse response)
        {
            Connection = connection;
            Response = response;

            StreamingCall = Connection.Bridge.Reader.readResultSet();
        }
        #endregion

        #region IEnumerator
        public JdbcDataRow Current { get; private set; }

        object IEnumerator.Current => Current;

        public bool MoveNext()
        {
            StreamingCall.RequestStream.WriteAsync(new ReadResultSetRequest
            {
                ResultSetId = Response.ResultSetId
            }).Wait();

            var result = StreamingCall.ResponseStream.MoveNext().Result;

            if (result)
                Current = StreamingCall.ResponseStream.Current;

            return result;
        }

        public void Reset()
        {
            throw new NotSupportedException();
        }
        #endregion

        #region IDisposable
        public void Dispose()
        {
            StreamingCall.RequestStream.CompleteAsync().Wait();
            StreamingCall?.Dispose();
        }
        #endregion
    }
}
