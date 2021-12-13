using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
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
        #endregion

        #region Constants
        private const string host = "127.0.0.1";
#if !DEBUG
        private const string jarPath = @"JDBC.NET.Bridge.jar";
#else
        private readonly string jarPath = "JDBC.NET.Bridge-1.0-SNAPSHOT-jar-with-dependencies.jar";
#endif
        #endregion

        #region Properties
        public DriverService.DriverServiceClient Driver { get; private set; }

        public ReaderService.ReaderServiceClient Reader { get; private set; }

        public StatementService.StatementServiceClient Statement { get; private set; }

        public DatabaseService.DatabaseServiceClient Database { get; private set; }

        public MetaDataService.MetaDataServiceClient MetaData { get; private set; }

        internal JdbcBridgePoolKey Key { get; }

        public string DriverPath { get; }

        public string DriverClass { get; }

        public int DriverMajorVersion { get; private set; }

        public int DriverMinorVersion { get; private set; }

        public JdbcConnectionProperties ConnectionProperties { get; }
        #endregion

        #region Constructor
        private JdbcBridge(string driverPath, string driverClass, JdbcConnectionProperties connectionProperties)
        {
            DriverPath = driverPath;
            DriverClass = driverClass;
            ConnectionProperties = connectionProperties;
            Key = JdbcBridgePoolKey.Create(driverPath, driverClass, connectionProperties);
        }
        #endregion

        #region Public Methods
        internal static JdbcBridge FromDriver(string driverPath, string driverClass, JdbcConnectionProperties connectionProperties)
        {
            var bridge = new JdbcBridge(driverPath, driverClass, connectionProperties);
            bridge.Initialize();

            return bridge;
        }
        #endregion

        #region Private Methods
        private void Initialize()
        {
            var port = PortUtility.GetFreeTcpPort();

            // TODO : Need to move Execute logic to J2NET
            var classPaths = string.Join(RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? ";" : ":", jarPath, DriverPath);
            var javaRunArgs = $"-XX:G1PeriodicGCInterval=5000";

            if (ConnectionProperties.TryGetValue("KRB5_CONFIG", out var krb5Config))
                javaRunArgs += $" -Djava.security.krb5.conf={krb5Config}";

            if (ConnectionProperties.TryGetValue("JAAS_CONFIG", out var jaasConfig))
                javaRunArgs += $" -Djava.security.auth.login.config={jaasConfig}";

            javaRunArgs += $" -cp \"{classPaths}\" com.chequer.jdbcnet.bridge.Main -p {port}";

            _process = JavaRuntime.Execute(javaRunArgs);
            _channel = new JdbcChannel(host, port, ChannelCredentials.Insecure);

            PortUtility.WaitForOpen(port);
            _channel.ConnectAsync().Wait();

            Driver = new DriverService.DriverServiceClient(_channel);
            Reader = new ReaderService.ReaderServiceClient(_channel);
            Statement = new StatementService.StatementServiceClient(_channel);
            Database = new DatabaseService.DatabaseServiceClient(_channel);
            MetaData = new MetaDataService.MetaDataServiceClient(_channel);

            var loadDriverResponse = Driver.loadDriver(new LoadDriverRequest
            {
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
