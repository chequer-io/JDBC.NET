using System.Data.Common;
using Grpc.Core;

namespace JDBC.NET.Data.Exceptions
{
    public class JdbcException : DbException
    {
        internal JdbcException(RpcException exception) : base(exception.Status.Detail)
        {
        }
    }
}
