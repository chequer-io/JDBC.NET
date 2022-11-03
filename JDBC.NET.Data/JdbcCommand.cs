using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using JDBC.NET.Data.Exceptions;
using JDBC.NET.Data.Utilities;
using JDBC.NET.Proto;

namespace JDBC.NET.Data
{
    public class JdbcCommand : DbCommand
    {
        #region Fields
        private bool _isDisposed;
        private JdbcDataReader _dataReader;
        private JdbcTransaction _dbTransaction;
        #endregion

        #region Properties
        public int FetchSize { get; set; }

        public override string CommandText { get; set; }

        public override int CommandTimeout { get; set; }

        public override CommandType CommandType { get; set; }

        public override UpdateRowSource UpdatedRowSource
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        protected override DbConnection DbConnection { get; set; }

        protected override DbParameterCollection DbParameterCollection => Parameters;

        public new JdbcParameterCollection Parameters { get; } = new();

        protected override DbTransaction DbTransaction
        {
            get => _dbTransaction;
            set
            {
                if (value is not JdbcTransaction jdbcTransaction)
                    throw new InvalidOperationException();

                if (Connection is not JdbcConnection jdbcConnection)
                    throw new InvalidOperationException();

                if (jdbcConnection.CurrentTransaction != jdbcTransaction)
                    throw new InvalidDataException("The transaction associated with this command is not the connection's active transaction.");

                _dbTransaction = jdbcTransaction;
            }
        }

        public override bool DesignTimeVisible
        {
            get => false;
            set => throw new NotSupportedException();
        }

        private bool IsPrepared { get; set; }

        private bool IsStatementCreated => StatementId is not null;

        private string StatementId { get; set; }
        #endregion

        #region Constructor
        internal JdbcCommand(JdbcConnection connection)
        {
            Connection = connection;
            FetchSize = connection.ConnectionStringBuilder.FetchSize;
        }
        #endregion

        #region Public Methods
        public override void Prepare()
        {
            IsPrepared = true;
        }

        public override int ExecuteNonQuery()
        {
            try
            {
                return ExecuteNonQueryAsync().Result;
            }
            catch (AggregateException e) when (e.InnerExceptions.Count == 1)
            {
                throw e.InnerExceptions[0];
            }
        }

        public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            cancellationToken.Register(Cancel);

            await using var dbDataReader = await ExecuteDbDataReaderAsync(CommandBehavior.Default, cancellationToken).ConfigureAwait(false);

            while (await dbDataReader.NextResultAsync(cancellationToken).ConfigureAwait(false))
            {
            }

            return dbDataReader.RecordsAffected;
        }

        public override object ExecuteScalar()
        {
            try
            {
                return ExecuteScalarAsync(CancellationToken.None).Result;
            }
            catch (AggregateException e) when (e.InnerExceptions.Count == 1)
            {
                throw e.InnerExceptions[0];
            }
        }

        public override async Task<object> ExecuteScalarAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            cancellationToken.Register(Cancel);
            object result = null;

            await using var dbDataReader = await ExecuteDbDataReaderAsync(CommandBehavior.Default, cancellationToken).ConfigureAwait(false);

            if (await dbDataReader.ReadAsync(cancellationToken).ConfigureAwait(false) && dbDataReader.FieldCount > 0)
            {
                result = dbDataReader.GetValue(0);
            }

            return result;
        }

        protected override DbParameter CreateDbParameter()
        {
            return new JdbcParameter();
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            try
            {
                return ExecuteDbDataReaderAsync(behavior, CancellationToken.None).Result;
            }
            catch (AggregateException e) when (e.InnerExceptions.Count == 1)
            {
                throw e.InnerExceptions[0];
            }
        }

        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            if (Connection is not JdbcConnection jdbcConnection)
                throw new InvalidOperationException();

            if (_dataReader?.IsClosed == false)
                throw new InvalidOperationException("The previously executed DataReader has not been closed yet.");

            CreateStatement();

            var response = await jdbcConnection.Bridge.Statement.executeStatementAsync(
                new ExecuteStatementRequest
                {
                    StatementId = StatementId,
                    FetchSize = FetchSize,
                    Sql = IsPrepared ? string.Empty : CommandText
                },
                cancellationToken: cancellationToken
            );

            _dataReader = new JdbcDataReader(this, response);
            return _dataReader;
        }

        public override void Cancel()
        {
            if (Connection is not JdbcConnection jdbcConnection)
                throw new InvalidOperationException();

            try
            {
                jdbcConnection.Bridge.Statement.cancelStatement(new CancelStatementRequest
                {
                    StatementId = StatementId
                });
            }
            catch (RpcException ex)
            {
                throw new JdbcException(ex);
            }
        }
        #endregion

        #region Private Methods
        private void CreateStatement()
        {
            if (IsPrepared)
            {
                CreatePreparedStatement();
                return;
            }

            if (Parameters.Count > 0)
            {
                CreatePreparedStatement();
                return;
            }
            
            CreateRawStatement();
        }
        
        private void CreatePreparedStatement()
        {
            if (Connection is not JdbcConnection jdbcConnection)
                throw new InvalidOperationException();
            
            CloseStatement();
            
            List<JdbcParameter> orderedParameters = Parameters
                .OfType<JdbcParameter>()
                .OrderBy(x => CommandText.IndexOf(x.ParameterName, StringComparison.Ordinal))
                .ToList();

            var response = jdbcConnection.Bridge.Statement.prepareStatement(new PrepareStatementRequest
            {
                ConnectionId = jdbcConnection.ConnectionId,
                Sql = orderedParameters.Aggregate(CommandText, (x, parameter) => x.Replace(parameter.ParameterName, "?"))
            });

            StatementId = response.StatementId;

            for (var i = 0; i < orderedParameters.Count; i++)
            {
                var parameter = orderedParameters[i];

                jdbcConnection.Bridge.Statement.setParameter(new SetParameterRequest
                {
                    StatementId = StatementId,
                    Index = i + 1,
                    Value = parameter.Value.ToString(),
                    Type = ParameterTypeUtility.Convert(parameter.DbType)
                });
            }
        }

        private void CreateRawStatement()
        {
            if (Connection is not JdbcConnection jdbcConnection)
                throw new InvalidOperationException();
            
            CloseStatement();

            var response = jdbcConnection.Bridge.Statement.createStatement(new CreateStatementRequest
            {
                ConnectionId = jdbcConnection.ConnectionId,
            });

            StatementId = response.StatementId;
        }

        private void CloseStatement()
        {
            if (!IsStatementCreated)
                return;

            if (Connection is not JdbcConnection jdbcConnection)
                throw new InvalidOperationException();

            jdbcConnection.Bridge.Statement.closeStatement(new CloseStatementRequest
            {
                StatementId = StatementId
            });

            StatementId = null;
        }
        #endregion

        #region IDisposable
        protected override void Dispose(bool disposing)
        {
            if (_isDisposed)
                return;

            var dbConnection = this.Connection;
            if (dbConnection is not null)
            {
                if (dbConnection.State is ConnectionState.Open or ConnectionState.Executing or ConnectionState.Fetching)
                    CloseStatement();
            }
            _isDisposed = true;

            base.Dispose(disposing);
        }
        #endregion
    }
}
