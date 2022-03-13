using System;
using System.Threading;
using System.Threading.Tasks;

namespace JDBC.NET.Data
{
    internal sealed class JdbcBridgePort : IDisposable
    {
        public event EventHandler Completed;

        public string Id { get; }

        public int ServerPort { get; }

        private CancellationTokenRegistration _cancellationTokenRegistration;
        private readonly TaskCompletionSource<ushort> _taskCompletionSource;

        public JdbcBridgePort(string id, int serverPort, CancellationToken cancellationToken)
        {
            Id = id;
            ServerPort = serverPort;

            if (cancellationToken != default)
                _cancellationTokenRegistration = cancellationToken.Register(Cancel);

            _taskCompletionSource = new TaskCompletionSource<ushort>();
        }

        private void Cancel()
        {
            if (_taskCompletionSource.TrySetCanceled())
                Complete();
        }

        public void SetResult(ushort port)
        {
            _taskCompletionSource.TrySetResult(port);
            Complete();
        }

        private void Complete()
        {
            _cancellationTokenRegistration.Dispose();
            _cancellationTokenRegistration = default;

            Completed?.Invoke(this, EventArgs.Empty);
        }

        public ushort GetPort()
        {
            try
            {
                return _taskCompletionSource.Task.Result;
            }
            catch (AggregateException e) when (e.InnerExceptions.Count == 1)
            {
                throw e.InnerExceptions[0];
            }
        }

        public void Dispose()
        {
            Cancel();
        }
    }
}
