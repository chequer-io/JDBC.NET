using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace JDBC.NET.Data.Utilities
{
    public static class SocketUtility
    {
        public static void WaitForOpen(Socket socket, EndPoint endPoint, int retryCount = 5, int retryInterval = 5000)
        {
            while (retryCount > 0)
            {
                try
                {
                    socket.Connect(endPoint);

                    if (IsConnected(socket))
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

        public static bool IsConnected(Socket socket)
        {
            return !(socket.Poll(1000, SelectMode.SelectRead) && socket.Available == 0 || !socket.Connected);
        }
    }
}
