using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
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

            var processInfo = JavaRuntime.Create(javaRunArgs);
            processInfo.RedirectStandardOutput = true;
            processInfo.RedirectStandardError = true;
            processInfo.UseShellExecute = false;

            var process = Process.Start(processInfo);

            if (process is null)
                throw new IOException("Failed to start JDBC.NET process");

            process.OutputDataReceived += (sender, args) => JdbcEventSource.Log.StandardOutputDataReceived(args.Data);
            process.ErrorDataReceived += (sender, args) => JdbcEventSource.Log.StandardErrorDataReceived(args.Data);

            process.BeginOutputReadLine();
            process.BeginErrorReadLine();

            process.EnableRaisingEvents = true;
            process.Exited += delegate { bridgeCTS.Cancel(); };

            if (process.HasExited)
                bridgeCTS.Cancel();

            try
            {
                var port = bridgePort.GetPort();

                var options = new GrpcChannelOptions
                {
                    MaxReceiveMessageSize = null,
                    MaxSendMessageSize = null,
                    DisposeHttpClient = true,
                };

                var channel = GrpcChannel.ForAddress($"http://{host}:{port}", options);
                var channelWithInterceptor = channel.Intercept(new JdbcCallInterceptor());

                Driver = new DriverService.DriverServiceClient(channelWithInterceptor);
                Reader = new ReaderService.ReaderServiceClient(channelWithInterceptor);
                Statement = new StatementService.StatementServiceClient(channelWithInterceptor);
                Database = new DatabaseService.DatabaseServiceClient(channelWithInterceptor);
                MetaData = new MetaDataService.MetaDataServiceClient(channelWithInterceptor);

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
            var exeLoc = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

            var mainJarLocation = Path.Join(exeLoc, jarPath);

            if (!File.Exists(mainJarLocation))
                throw new FileNotFoundException(@$"'{jarPath}' not found at '{mainJarLocation}'");

            var resolvedOptionsDriverPath = Options.DriverPath;

            //check if Options.DriverPath is the actual full path to the file
            if (!File.Exists(resolvedOptionsDriverPath))
            {
                //maybe Options.DriverPath is a relative driver path
                resolvedOptionsDriverPath = Path.Join(exeLoc, Options.DriverPath);

                if (!File.Exists(resolvedOptionsDriverPath))
                    throw new FileNotFoundException($"'{Options.DriverPath}' and '{resolvedOptionsDriverPath}' not found!");
            }

            var defaultJarFiles = new[] { mainJarLocation, resolvedOptionsDriverPath };

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
