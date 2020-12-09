using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using JDBC.NET.Data.Converters;
using JDBC.NET.Data.Models;
using JDBC.NET.Proto;

namespace JDBC.NET.Data
{
    public class JdbcConnection : DbConnection, ICloneable
    {
        #region Fields
        private string _database;
        private string _serverVersion;
        private ConnectionState _state = ConnectionState.Closed;
        private bool _isDisposed;
        #endregion

        #region Properties
        public override string Database
        {
            get
            {
                CheckOpen();
                return _database;
            }
        }

        public override string DataSource => ConnectionStringBuilder.JdbcUrl;

        public override string ServerVersion
        {
            get
            {
                CheckOpen();
                return _serverVersion;
            }
        }

        public override string ConnectionString
        {
            get => ConnectionStringBuilder.ToString();
            set => ConnectionStringBuilder = new JdbcConnectionStringBuilder(value);
        }

        public JdbcConnectionProperties ConnectionProperties { get; set; } = new JdbcConnectionProperties();

        internal JdbcConnectionStringBuilder ConnectionStringBuilder { get; set; } = new JdbcConnectionStringBuilder();

        public override ConnectionState State => _state;

        internal string ConnectionId { get; private set; }

        internal JdbcTransaction CurrentTransaction { get; private set; }

        internal JdbcBridge Bridge { get; private set; }
        #endregion

        #region Constructor
        public JdbcConnection()
        {
        }

        public JdbcConnection(string connectionString, IEnumerable<KeyValuePair<string, string>> connectionProperties = null) : this(new JdbcConnectionStringBuilder(connectionString), connectionProperties)
        {
        }

        public JdbcConnection(JdbcConnectionStringBuilder connectionStringBuilder, IEnumerable<KeyValuePair<string, string>> connectionProperties = null) : this()
        {
            ConnectionStringBuilder = connectionStringBuilder;
            ConnectionProperties = new JdbcConnectionProperties(connectionProperties);
        }
        #endregion

        #region Public Methods
        public override DataTable GetSchema()
        {
            return null;
        }

        public override DataTable GetSchema(string collectionName)
        {
            return null;
        }

        public override DataTable GetSchema(string collectionName, string[] restrictionValues)
        {
            return null;
        }

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

                    Bridge = JdbcBridgePool.Lease(ConnectionStringBuilder.DriverPath, ConnectionStringBuilder.DriverClass);

                    var response = Bridge.Database.openConnection(new OpenConnectionRequest
                    {
                        JdbcUrl = ConnectionStringBuilder.JdbcUrl,
                        Properties =
                        {
                            ConnectionProperties
                        }
                    });

                    ConnectionId = response.ConnectionId;
                    _serverVersion = response.DatabaseProductVersion;
                    _database = response.Catalog;

                    _state = ConnectionState.Open;
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
                    if (_state != ConnectionState.Closed && Bridge != null)
                    {
                        Bridge.Database.closeConnection(new CloseConnectionRequest
                        {
                            ConnectionId = ConnectionId
                        });

                        JdbcBridgePool.Release(Bridge.Key);
                    }

                    _state = ConnectionState.Closed;
                }
                catch
                {
                    _state = ConnectionState.Broken;
                    throw;
                }
            });
        }

        public new JdbcCommand CreateCommand()
        {
            return (JdbcCommand)base.CreateCommand();
        }

        public JdbcCommand CreateCommand(string commandText)
        {
            var command = CreateCommand();
            command.CommandText = commandText;

            return command;
        }

        protected override DbCommand CreateDbCommand()
        {
            CheckOpen();
            return new JdbcCommand(this);
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            CheckOpen();

            if (CurrentTransaction?.IsDisposeed == false)
                throw new InvalidOperationException("A transaction is already in progress. Nested transactions are not supported.");

            Bridge.Database.setAutoCommit(new SetAutoCommitRequest
            {
                ConnectionId = ConnectionId,
                UseAutoCommit = false
            });

            var originalLevel = Bridge.Database.getTransactionIsolation(new GetTransactionIsolationRequest
            {
                ConnectionId = ConnectionId
            }).Isolation;

            if (isolationLevel == IsolationLevel.Unspecified)
            {
                isolationLevel = originalLevel switch
                {
                    TransactionIsolation.None => IsolationLevel.Unspecified,
                    TransactionIsolation.ReadCommitted => IsolationLevel.ReadCommitted,
                    TransactionIsolation.ReadUncommitted => IsolationLevel.ReadUncommitted,
                    TransactionIsolation.RepeatableRead => IsolationLevel.RepeatableRead,
                    TransactionIsolation.Serializable => IsolationLevel.Serializable,
                    _ => throw new ArgumentOutOfRangeException()
                };
            }
            else if (IsolationLevelConverter.Convert(isolationLevel) != originalLevel)
            {
                Bridge.Database.setTransactionIsolation(new SetTransactionIsolationRequest
                {
                    ConnectionId = ConnectionId,
                    Isolation = IsolationLevelConverter.Convert(isolationLevel)
                });
            }

            CurrentTransaction = new JdbcTransaction(this, isolationLevel, originalLevel);

            return CurrentTransaction;
        }

        public override void ChangeDatabase(string databaseName)
        {
            CheckOpen();

            Bridge.Database.changeCatalog(new ChangeCatalogRequest
            {
                ConnectionId = ConnectionId,
                CatalogName = databaseName
            });

            _database = databaseName;
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

            if (disposing)
                Close();

            _isDisposed = true;

            base.Dispose(disposing);
        }
        #endregion

        #region ICloneable
        public object Clone()
        {
            CheckDispose();
            return new JdbcConnection(ConnectionStringBuilder, ConnectionProperties);
        }
        #endregion
    }
}
