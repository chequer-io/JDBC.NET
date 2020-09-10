using System;
using System.Diagnostics;
using Grpc.Core;
using J2NET;
using JDBC.NET.Data.Utilities;
using JDBC.NET.Proto;

namespace JDBC.NET.Data.Models
{
    internal sealed class JdbcBridge : IDisposable
    {
        #region Fields
        private Channel _channel;
        private Process _process;
        private DriverService.DriverServiceClient _driverService;
        #endregion

        #region Constants
        private const string host = "127.0.0.1";
#if !DEBUG
        private const string jarPath = @"JDBC.NET.Bridge-1.0.jar";
#else
        private const string jarPath = @"..\..\..\..\JDBC.NET.Bridge\target\JDBC.NET.Bridge-1.0-SNAPSHOT-jar-with-dependencies.jar";
#endif
        #endregion

        #region Properties
        public string Key => GenerateKey(DriverPath, DriverClass);

        public string DriverPath { get; }

        public string DriverClass { get; }

        public int DriverMajorVersion { get; private set; }

        public int DriverMinorVersion { get; private set; }
        #endregion

        #region Constructor
        private JdbcBridge(string driverPath, string driverClass)
        {
            DriverPath = driverPath;
            DriverClass = driverClass;
        }
        #endregion

        #region Public Methods
        internal static string GenerateKey(string driverPath, string driverClass)
        {
            return $"{driverPath}:{driverClass}";
        }

        internal static JdbcBridge FromDriver(string driverPath, string driverClass)
        {
            var bridge = new JdbcBridge(driverPath, driverClass);
            bridge.Initialize();

            return bridge;
        }
        #endregion

        #region Private Methods
        private void Initialize()
        {
            var port = PortUtility.GetFreeTcpPort();
            _process = JavaRuntime.ExecuteJar(jarPath, $"-p {port}");
            PortUtility.WaitForOpen(port);

            _channel = new Channel($"{host}:{port}", ChannelCredentials.Insecure);
            _driverService = new DriverService.DriverServiceClient(_channel);

            var loadDriverResponse = _driverService.loadDriver(new LoadDriverRequest
            {
                Path = DriverPath,
                ClassName = DriverClass
            });

            DriverMajorVersion = loadDriverResponse.MajorVersion;
            DriverMinorVersion = loadDriverResponse.MinorVersion;
        }
        #endregion

        #region IDisposable
        public void Dispose()
        {
            _process?.Kill();
            _process?.Dispose();
        }
        #endregion
    }
}
