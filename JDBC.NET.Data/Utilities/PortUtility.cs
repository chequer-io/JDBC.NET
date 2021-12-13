using System.Net;
using System.Net.Sockets;
using System.Runtime.ExceptionServices;

namespace JDBC.NET.Data.Utilities
{
    public static class PortUtility
    {
        public static void WaitForOpen(int port, int retryCount = 30, int retryInterval = 500)
        {
            do
            {
                var endPoint = new IPEndPoint(IPAddress.Loopback, port);
                using var socket = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

                try
                {
                    socket.Blocking = false;
                    socket.Connect(endPoint);
                    socket.Blocking = true;

                    break;
                }
                catch (SocketException e)
                {
                    if (e.SocketErrorCode != SocketError.WouldBlock)
                    {
                        socket.Close();

                        if (retryCount <= 0)
                            ExceptionDispatchInfo.Throw(e);

                        continue;
                    }

                    var microSeconds = retryInterval * 1000;

                    if (!socket.Poll(microSeconds, SelectMode.SelectWrite))
                    {
                        socket.Close();

                        if (retryCount <= 0)
                            throw new SocketException((int)SocketError.TimedOut);

                        continue;
                    }

                    socket.Blocking = true;
                    break;
                }
            } while (--retryCount > 0);
        }

        public static int GetFreeTcpPort()
        {
            int port;
            var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

            try
            {
                var localEP = new IPEndPoint(IPAddress.Any, 0);
                socket.Bind(localEP);
                localEP = (IPEndPoint)socket.LocalEndPoint;
                port = localEP?.Port ?? default;
            }
            finally
            {
                socket.Close();
            }

            return port;
        }
    }
}
