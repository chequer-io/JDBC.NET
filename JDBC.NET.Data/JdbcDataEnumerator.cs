using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Grpc.Core;
using JDBC.NET.Proto;

namespace JDBC.NET.Data
{
    public class JdbcDataEnumerator : IEnumerator<DataRow>
    {
        #region Properties
        private JdbcConnection Connection { get; }

        private ExecuteStatementResponse Response { get; }

        private AsyncServerStreamingCall<DataRow> StreamingCall { get; }
        #endregion

        #region Constructor
        public JdbcDataEnumerator(JdbcConnection connection, ExecuteStatementResponse response)
        {
            Connection = connection;
            Response = response;

            StreamingCall = Connection.Bridge.Reader.readResultSet(new ReadResultSetRequest
            {
                ResultSetId = Response.ResultSetId
            });
        }
        #endregion

        #region IEnumerator
        public DataRow Current { get; private set; }

        object IEnumerator.Current => Current;

        public bool MoveNext()
        {
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
            StreamingCall?.Dispose();
        }
        #endregion
    }
}
