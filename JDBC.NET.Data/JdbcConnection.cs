using System;
using System.Data;
using System.Data.Common;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using JDBC.NET.Data.Models;

namespace JDBC.NET.Data
{
    public class JdbcConnection : DbConnection, ICloneable
    {
        #region Fields
        private string _schema;
        private string _database;
        private string _serverVersion;
        private ConnectionState _state = ConnectionState.Closed;
        private JdbcBridge _bridge;
        private bool _isDisposed;
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

                    _bridge = JdbcBridgePool.Lease(ConnectionStringBuilder.DriverPath, ConnectionStringBuilder.DriverClass);

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
            JdbcBridgePool.Release(_bridge.Key);

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
        private void CheckOpen()
        {
            CheckDispose();

            if (_state == ConnectionState.Closed || _state == ConnectionState.Broken)
                throw new InvalidOperationException("Connection is not open.");
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
