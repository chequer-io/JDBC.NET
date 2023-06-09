using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Threading;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using J2NET;
using JDBC.NET.Proto;

namespace JDBC.NET.Data.Models
{
    internal sealed class JdbcBridge : IDisposable
    {
        #region Fields
        private GrpcChannel _channel;
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
                var options = new GrpcChannelOptions
                {
                    MaxSendMessageSize = null,
                    MaxReceiveMessageSize = null,
#if NET6_0_OR_GREATER
                    HttpHandler = new SocketsHttpHandler
                    {
                        PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
                        KeepAlivePingDelay = TimeSpan.FromSeconds(60),
                        KeepAlivePingTimeout = TimeSpan.FromSeconds(30),
                        EnableMultipleHttp2Connections = true
                    }
#endif
                };

                var channel = GrpcChannel.ForAddress($"http://{host}:{port}", options);
                var jdbcCallInvoker = channel.Intercept(new JdbcInterceptor());

                Driver = new DriverService.DriverServiceClient(jdbcCallInvoker);
                Reader = new ReaderService.ReaderServiceClient(jdbcCallInvoker);
                Statement = new StatementService.StatementServiceClient(jdbcCallInvoker);
                Database = new DatabaseService.DatabaseServiceClient(jdbcCallInvoker);
                MetaData = new MetaDataService.MetaDataServiceClient(jdbcCallInvoker);

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

            IEnumerable<string> libraryJarFiles = Options.LibraryJarFiles ?? Enumerable.Empty<string>();
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
