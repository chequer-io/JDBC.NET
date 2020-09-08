using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using J2NET;
using JDBC.NET.Data.Utilities;
using JDBC.NET.Proto;

namespace JDBC.NET.Data
{
    public class JdbcConnection : DbConnection, ICloneable
    {
        #region Fields
        private string _schema;
        private string _database;
        private string _serverVersion;
        private ConnectionState _state = ConnectionState.Closed;
        private bool _isDisposed;
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
        public string Schema => _schema;

        public override string Database => _database;

        public override string DataSource => ConnectionStringBuilder.JdbcConnectionString;

        public override string ServerVersion
        {
            get
            {
                CheckOpen();

                if (_serverVersion == null)
                {
                    try
                    {
                        var command = CreateDbCommand("SELECT node_version FROM system.runtime.nodes");
                        var version = command.ExecuteScalar().ToString();
                        var match = Regex.Match(version, @"\d+\.\d+");

                        _serverVersion = match.Success ? match.Value : $"0.{version}";
                    }
                    catch
                    {
                        return "0.0";
                    }
                }

                return _serverVersion;
            }
        }

        public override string ConnectionString
        {
            get => ConnectionStringBuilder.ToString();
            set => ConnectionStringBuilder = new JdbcConnectionStringBuilder(value);
        }

        private JdbcConnectionStringBuilder ConnectionStringBuilder { get; set; } = new JdbcConnectionStringBuilder();

        public override ConnectionState State => _state;

        private BridgeService.BridgeServiceClient BridgeService { get; set; }
        #endregion

        #region Constructor
        public JdbcConnection()
        {
        }

        public JdbcConnection(string connectionString) : this(new JdbcConnectionStringBuilder(connectionString))
        {
        }

        public JdbcConnection(JdbcConnectionStringBuilder connectionStringBuilder) : this()
        {
            ConnectionStringBuilder = connectionStringBuilder;
        }
        #endregion

        #region Public Methods
        public override void Open()
        {
            try
            {
                OpenAsync().Wait();
            }
            catch (AggregateException e)
            {
                var innerException = e.Flatten().InnerException;

                if (innerException != null)
                    throw innerException;
            }
        }

        public override async Task OpenAsync(CancellationToken cancellationToken)
        {
            CheckDispose();

            await Task.Run(() =>
            {
                try
                {
                    _state = ConnectionState.Connecting;

                    InitializeBridge();
                    CheckBridgeOpen();

                    var loadDriverResponse = BridgeService.loadDriver(new LoadDriverRequest
                    {
                        Path = ConnectionStringBuilder.DriverPath,
                        ClassName = ConnectionStringBuilder.DriverClass
                    });

                    _state = ConnectionState.Open;

                    /*
                    ChangeDatabase(_prestoClientSessionConfig.Catalog);
                    ChangeSchema(_prestoClientSessionConfig.Schema);
                    */
                }
                catch
                {
                    _state = ConnectionState.Broken;
                    throw;
                }
            }, cancellationToken);
        }

        public override void Close()
        {
            try
            {
                CloseAsync().Wait();
            }
            catch (AggregateException e)
            {
                var innerException = e.Flatten().InnerException;

                if (innerException != null)
                    throw innerException;
            }
        }

        public override async Task CloseAsync()
        {
            CheckDispose();

            await Task.Run(() =>
            {
                try
                {
                    _state = ConnectionState.Closed;
                }
                catch
                {
                    _state = ConnectionState.Broken;
                    throw;
                }
            });
        }

        protected override DbCommand CreateDbCommand()
        {
            CheckOpen();

            /*
            return new PrestoCommand
            {
                Connection = this
            };
            */

            throw new NotImplementedException();
        }

        public DbCommand CreateDbCommand(string commandText)
        {
            var command = CreateDbCommand();
            command.CommandText = commandText;

            return command;
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotImplementedException();
        }

        public override void ChangeDatabase(string databaseName)
        {
            CheckOpen();
            //_prestoClientSessionConfig.Catalog = databaseName;
            _database = databaseName;
        }

        public void ChangeSchema(string schemaName)
        {
            CheckOpen();
            //_prestoClientSessionConfig.Schema = schemaName;
            _schema = schemaName;
        }
        #endregion

        #region Private Methods
        private void InitializeBridge()
        {
            var port = PortUtility.GetFreeTcpPort();
            _process = JavaRuntime.ExecuteJar(bridgeJar, $"-p {port}");
            PortUtility.WaitForOpen(port);

            _channel = new Channel($"127.0.0.1:{port}", ChannelCredentials.Insecure);
            BridgeService = new BridgeService.BridgeServiceClient(_channel);
        }

        private void CheckOpen()
        {
            CheckDispose();

            if (_state == ConnectionState.Closed || _state == ConnectionState.Broken)
                throw new InvalidOperationException("Connection is not open.");
        }

        private void CheckBridgeOpen()
        {
            CheckDispose();

            if (_process == null || _channel == null || BridgeService == null)
                throw new InvalidOperationException("Bridge is not open.");
        }

        private void CheckDispose()
        {
            if (_isDisposed)
                throw new ObjectDisposedException(ToString());
        }
        #endregion

        #region IDisposable
        protected override void Dispose(bool disposing)
        {
            if (_isDisposed)
                return;

            Close();

            _channel?.ShutdownAsync().Wait();

            _process?.Kill();
            _process?.Dispose();

            _isDisposed = true;

            base.Dispose(disposing);
        }
        #endregion

        #region ICloneable
        public object Clone()
        {
            CheckDispose();
            return new JdbcConnection(ConnectionStringBuilder);
        }
        #endregion
    }
}
