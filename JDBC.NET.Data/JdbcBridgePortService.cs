using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using JDBC.NET.Data.Utilities;

namespace JDBC.NET.Data
{
    internal static class JdbcBridgePortService
    {
        private static readonly ConcurrentStack<SocketAsyncEventArgs> _saeaPool = new();
        private static readonly ConcurrentDictionary<string, JdbcBridgePort> _ports = new();

        private static Socket _server;

        private static void Initialize()
        {
            lock (typeof(JdbcBridgePortService))
            {
                if (_server is not null)
                    return;

                _server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                _server.Bind(new IPEndPoint(IPAddress.Any, 0));
                _server.Listen();

                _server.BeginAccept(Accept, null);
            }
        }

        private static void Accept(IAsyncResult ar)
        {
            var accept = _server.EndAccept(ar);
            _server.BeginAccept(Accept, null);

            ProcessAccept(accept);
        }

        private static void ProcessAccept(Socket accept)
        {
            if (!_saeaPool.TryPop(out var recvArgs))
            {
                recvArgs = new SocketAsyncEventArgs();

                // ID(10) | PORT(2)
                recvArgs.SetBuffer(new byte[12]);
            }

            recvArgs.UserToken = accept;
            recvArgs.Completed += RecvArgsOnCompleted;

            if (!accept.ReceiveAsync(recvArgs))
                ProcessReceive(recvArgs);
        }

        private static void RecvArgsOnCompleted(object sender, SocketAsyncEventArgs e)
        {
            ProcessReceive(e);
        }

        private static void ProcessReceive(SocketAsyncEventArgs recvArgs)
        {
            if (recvArgs.BytesTransferred == 12 &&
                recvArgs.SocketError == SocketError.Success)
            {
                var id = Encoding.ASCII.GetString(recvArgs.MemoryBuffer.Span[..10]);
                var port = BinaryPrimitives.ReadUInt16LittleEndian(recvArgs.MemoryBuffer.Span[10..]);

                if (_ports.TryGetValue(id, out var bridgePort))
                    bridgePort.SetResult(port);
            }

            Complete(recvArgs);
        }

        private static void Complete(SocketAsyncEventArgs recvArgs)
        {
            if (recvArgs.UserToken is Socket socket)
                socket.Dispose();

            recvArgs.UserToken = null;
            recvArgs.Completed -= RecvArgsOnCompleted;

            _saeaPool.Push(recvArgs);
        }

        public static JdbcBridgePort Create(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            Initialize();

            var id = StringUtility.CreateRandom(10);
            var serverPort = ((IPEndPoint)_server.LocalEndPoint)!.Port;
            var bridgePort = new JdbcBridgePort(id, serverPort, cancellationToken);

            _ports[id] = bridgePort;
            bridgePort.Completed += BridgePortOnCompleted;

            return bridgePort;
        }

        private static void BridgePortOnCompleted(object sender, EventArgs e)
        {
            var port = (JdbcBridgePort)sender;
            port.Completed -= BridgePortOnCompleted;
            _ports.TryRemove(port.Id, out _);
        }
    }
}
