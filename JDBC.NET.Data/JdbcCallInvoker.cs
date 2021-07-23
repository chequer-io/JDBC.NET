using Grpc.Core;
using JDBC.NET.Data.Exceptions;

namespace JDBC.NET.Data
{
    internal sealed class JdbcCallInvoker : DefaultCallInvoker
    {
        public JdbcCallInvoker(Channel channel) : base(channel)
        {
        }

        public override TResponse BlockingUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string host, CallOptions options, TRequest request)
        {
            try
            {
                return base.BlockingUnaryCall(method, host, options, request);
            }
            catch (RpcException e)
            {
                throw new JdbcException(e);
            }
        }

        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string host, CallOptions options, TRequest request)
        {
            try
            {
                return base.AsyncUnaryCall(method, host, options, request);
            }
            catch (RpcException e)
            {
                throw new JdbcException(e);
            }
        }

        public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string host, CallOptions options)
        {
            try
            {
                return base.AsyncClientStreamingCall(method, host, options);
            }
            catch (RpcException e)
            {
                throw new JdbcException(e);
            }
        }

        public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string host, CallOptions options)
        {
            try
            {
                return base.AsyncDuplexStreamingCall(method, host, options);
            }
            catch (RpcException e)
            {
                throw new JdbcException(e);
            }
        }

        public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string host, CallOptions options, TRequest request)
        {
            try
            {
                return base.AsyncServerStreamingCall(method, host, options, request);
            }
            catch (RpcException e)
            {
                throw new JdbcException(e);
            }
        }
    }
}
