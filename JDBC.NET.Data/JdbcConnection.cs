using System;
using System.Diagnostics;
using System.Net.Sockets;
using System.Threading;
using Grpc.Core;
using J2NET;
using JDBC.NET.Data.Utilities;
using JDBC.NET.Proto;

namespace JDBC.NET.Data
{
    public class JdbcConnection : IDisposable
    {
        #region Fields
        private Process _process;
        private Channel _channel;
        #endregion

        #region Constants
#if !DEBUG
        private const string bridgeJar = @"JDBC.NET.Bridge-1.0.jar";
#else
        private const string bridgeJar = @"..\..\..\..\JDBC.NET.Bridge\target\JDBC.NET.Bridge-1.0-SNAPSHOT-jar-with-dependencies.jar";
#endif
        #endregion

        #region Properties
        private BridgeService.BridgeServiceClient BridgeService { get; set; }
        #endregion

        #region Private Methods
        private void Initialize()
        {
            var port = PortUtility.GetFreeTcpPort();
            _process = JavaRuntime.ExecuteJar(bridgeJar, $"-p {port}");
            PortUtility.WaitForOpen(port);

            _channel = new Channel($"127.0.0.1:{port}", ChannelCredentials.Insecure);
            BridgeService = new BridgeService.BridgeServiceClient(_channel);
        }
        #endregion

        #region Public Methods
        public LoadDriverResponse LoadDriver(string path, string className)
        {
            Initialize();

            return BridgeService.loadDriver(new LoadDriverRequest
            {
                Path = path,
                ClassName = className
            });
        }
        #endregion

        #region IDisposable
        public void Dispose()
        {
            _channel?.ShutdownAsync().Wait();

            _process?.Kill();
            _process?.Dispose();
        }
        #endregion
    }
}
