using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using Grpc.Core;
using J2NET;
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
        private const string jarPath = "JDBC.NET.Bridge-1.0-SNAPSHOT-jar-with-dependencies.jar";
#endif
        #endregion

        #region Properties
        public DriverService.DriverServiceClient Driver { get; private set; }

        public ReaderService.ReaderServiceClient Reader { get; private set; }

        public StatementService.StatementServiceClient Statement { get; private set; }

        public DatabaseService.DatabaseServiceClient Database { get; private set; }

        public MetaDataService.MetaDataServiceClient MetaData { get; private set; }

        internal JdbcBridgePoolKey Key { get; }

        public int DriverMajorVersion { get; private set; }

        public int DriverMinorVersion { get; private set; }

        public JdbcBridgeOptions Options { get; set; }
        #endregion

        #region Constructor
        private JdbcBridge(JdbcBridgeOptions options)
        {
            Options = options;
            Key = JdbcBridgePoolKey.Create(options);
        }
        #endregion

        #region Public Methods
        internal static JdbcBridge FromDriver(JdbcBridgeOptions options)
        {
            var bridge = new JdbcBridge(options);
            bridge.Initialize();

            return bridge;
        }
        #endregion

        #region Private Methods
        private void Initialize()
        {
            var bridgeCTS = new CancellationTokenSource();
            using var bridgePort = JdbcBridgePortService.Create(bridgeCTS.Token);

            var classPaths = string.Join(RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? ";" : ":", ResolveJarFiles());
            var javaRunArgs = $"-XX:G1PeriodicGCInterval=5000";

            if (Options.ConnectionProperties.TryGetValue("KRB5_CONFIG", out var krb5Config))
                javaRunArgs += $" -Djava.security.krb5.conf={krb5Config}";

            if (Options.ConnectionProperties.TryGetValue("JAAS_CONFIG", out var jaasConfig))
                javaRunArgs += $" -Djava.security.auth.login.config={jaasConfig}";

            javaRunArgs += $" -cp \"{classPaths}\" com.chequer.jdbcnet.bridge.Main -i {bridgePort.Id} -p {bridgePort.ServerPort}";

            var process = JavaRuntime.Execute(javaRunArgs);
            process.EnableRaisingEvents = true;
            process.Exited += delegate { bridgeCTS.Cancel(); };

            if (process.HasExited)
                bridgeCTS.Cancel();

            try
            {
                var port = bridgePort.GetPort();

                var channel = new JdbcChannel(host, port, ChannelCredentials.Insecure);
                channel.ConnectAsync().Wait(CancellationToken.None);

                Driver = new DriverService.DriverServiceClient(channel);
                Reader = new ReaderService.ReaderServiceClient(channel);
                Statement = new StatementService.StatementServiceClient(channel);
                Database = new DatabaseService.DatabaseServiceClient(channel);
                MetaData = new MetaDataService.MetaDataServiceClient(channel);

                var loadDriverResponse = Driver.loadDriver(
                    new LoadDriverRequest
                    {
                        ClassName = Options.DriverClass
                    }
                );

                DriverMajorVersion = loadDriverResponse.MajorVersion;
                DriverMinorVersion = loadDriverResponse.MinorVersion;

                _process = process;
                _channel = channel;
            }
            catch
            {
                process.Kill();
                process.Dispose();
                throw;
            }
        }

        private IEnumerable<string> ResolveJarFiles()
        {
            var defaultJarFiles = new[] { jarPath, Options.DriverPath };

            if (string.IsNullOrEmpty(Options.LibraryJarFiles))
                return defaultJarFiles;

            IEnumerable<string> libraryJarFiles = Options.LibraryJarFiles.Split(',').Select(path => path.Trim());
            return defaultJarFiles.Concat(libraryJarFiles);
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
