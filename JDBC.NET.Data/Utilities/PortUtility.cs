using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace JDBC.NET.Data.Utilities
{
    public static class PortUtility
    {
        public static void WaitForOpen(int port, int retryCount = 5, int retryInterval = 5000)
        {
            using var tcpClient = new TcpClient();

            while (retryCount > 0)
            {
                try
                {
                    tcpClient.Connect("127.0.0.1", port);
                    break;
                }
                catch (Exception)
                {
                    Thread.Sleep(retryInterval);
                }

                retryCount--;
            }

            if (retryCount <= 0)
                throw new TimeoutException();
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
                port = localEP.Port;
            }
            finally
            {
                socket.Close();
            }

            return port;
        }
    }
}
