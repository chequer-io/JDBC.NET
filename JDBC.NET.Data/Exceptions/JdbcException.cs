using System;
using System.Data.Common;
using Grpc.Core;

namespace JDBC.NET.Data.Exceptions
{
    public class JdbcException : DbException
    {
        public JdbcException(RpcException exception) : base(exception.Status.Detail)
        {
        }

        #region Static Methods
        public static void Try(Action action)
        {
            try
            {
                action();
            }
            catch (RpcException ex)
            {
                throw new JdbcException(ex);
            }
        }

        public static T Try<T>(Func<T> func)
        {
            try
            {
                return func();
            }
            catch (RpcException ex)
            {
                throw new JdbcException(ex);
            }
        }
        #endregion
    }
}
