using System.Collections.Generic;
using Grpc.Core;

namespace JDBC.NET.Data
{
    internal sealed class JdbcChannel : Channel
    {
        public JdbcChannel(string target, ChannelCredentials credentials) : base(target, credentials)
        {
        }

        public JdbcChannel(string target, ChannelCredentials credentials, IEnumerable<ChannelOption> options) : base(target, credentials, options)
        {
        }

        public JdbcChannel(string host, int port, ChannelCredentials credentials) : base(host, port, credentials)
        {
        }

        public JdbcChannel(string host, int port, ChannelCredentials credentials, IEnumerable<ChannelOption> options) : base(host, port, credentials, options)
        {
        }

        public override CallInvoker CreateCallInvoker()
        {
            return new JdbcCallInvoker(this);
        }
    }
}
