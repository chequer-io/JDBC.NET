using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;
using JDBC.NET.Data.Exceptions;

namespace JDBC.NET.Data
{
    internal sealed class JdbcInterceptor : Interceptor
    {
        public override TResponse BlockingUnaryCall<TRequest, TResponse>(TRequest request, ClientInterceptorContext<TRequest, TResponse> context, BlockingUnaryCallContinuation<TRequest, TResponse> continuation)
        {
            try
            {
                return continuation(request, context);
            }
            catch (RpcException e)
            {
                throw new JdbcException(e);
            }
        }

        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(TRequest request, ClientInterceptorContext<TRequest, TResponse> context, AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
        {
            AsyncUnaryCall<TResponse> asyncCall = continuation(request, context);

            return new AsyncUnaryCall<TResponse>(
                Wrap(asyncCall.ResponseAsync),
                Wrap(asyncCall.ResponseHeadersAsync),
                asyncCall.GetStatus,
                asyncCall.GetTrailers,
                asyncCall.Dispose
            );
        }

        public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(ClientInterceptorContext<TRequest, TResponse> context, AsyncClientStreamingCallContinuation<TRequest, TResponse> continuation)
        {
            AsyncClientStreamingCall<TRequest, TResponse> asyncCall = continuation(context);

            return new AsyncClientStreamingCall<TRequest, TResponse>(
                Wrap(asyncCall.RequestStream),
                Wrap(asyncCall.ResponseAsync),
                Wrap(asyncCall.ResponseHeadersAsync),
                asyncCall.GetStatus,
                asyncCall.GetTrailers,
                asyncCall.Dispose
            );
        }

        public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(ClientInterceptorContext<TRequest, TResponse> context, AsyncDuplexStreamingCallContinuation<TRequest, TResponse> continuation)
        {
            AsyncDuplexStreamingCall<TRequest, TResponse> asyncCall = continuation(context);

            return new AsyncDuplexStreamingCall<TRequest, TResponse>(
                Wrap(asyncCall.RequestStream),
                Wrap(asyncCall.ResponseStream),
                Wrap(asyncCall.ResponseHeadersAsync),
                asyncCall.GetStatus,
                asyncCall.GetTrailers,
                asyncCall.Dispose
            );
        }

        public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(TRequest request, ClientInterceptorContext<TRequest, TResponse> context, AsyncServerStreamingCallContinuation<TRequest, TResponse> continuation)
        {
            AsyncServerStreamingCall<TResponse> asyncCall = continuation(request, context);

            return new AsyncServerStreamingCall<TResponse>(
                Wrap(asyncCall.ResponseStream),
                Wrap(asyncCall.ResponseHeadersAsync),
                asyncCall.GetStatus,
                asyncCall.GetTrailers,
                asyncCall.Dispose
            );
        }

        private static async Task<T> Wrap<T>(Task<T> task)
        {
            if (task.Status != TaskStatus.WaitingForActivation)
                return await task;

            try
            {
                return await task;
            }
            catch (RpcException e)
            {
                throw new JdbcException(e);
            }
        }

        private static IClientStreamWriter<T> Wrap<T>(IClientStreamWriter<T> writer)
        {
            if (writer == null)
                return null;

            return new JdbcClientStreamWriter<T>(writer);
        }

        private static IAsyncStreamReader<T> Wrap<T>(IAsyncStreamReader<T> reader)
        {
            if (reader == null)
                return null;

            return new JdbcAsyncStreamReader<T>(reader);
        }

        private readonly struct JdbcClientStreamWriter<T> : IClientStreamWriter<T>
        {
            public WriteOptions WriteOptions
            {
                get => _writer.WriteOptions;
                set => _writer.WriteOptions = value;
            }

            private readonly IClientStreamWriter<T> _writer;

            public JdbcClientStreamWriter(IClientStreamWriter<T> writer)
            {
                _writer = writer;
            }

            public async Task WriteAsync(T message)
            {
                try
                {
                    await _writer.WriteAsync(message);
                }
                catch (RpcException e)
                {
                    throw new JdbcException(e);
                }
            }

            public async Task CompleteAsync()
            {
                try
                {
                    await _writer.CompleteAsync();
                }
                catch (RpcException e)
                {
                    throw new JdbcException(e);
                }
            }
        }

        private readonly struct JdbcAsyncStreamReader<T> : IAsyncStreamReader<T>
        {
            public T Current => _reader.Current;

            private readonly IAsyncStreamReader<T> _reader;

            public JdbcAsyncStreamReader(IAsyncStreamReader<T> reader)
            {
                _reader = reader;
            }

            public async Task<bool> MoveNext(CancellationToken cancellationToken)
            {
                try
                {
                    return await _reader.MoveNext(cancellationToken);
                }
                catch (RpcException e)
                {
                    throw new JdbcException(e);
                }
            }
        }
    }
}