using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using JDBC.NET.Proto;

namespace JDBC.NET.Data
{
    public class JdbcCommand : DbCommand
    {
        #region Fields
        private bool _isDisposed;
        #endregion

        #region Properties
        public override string CommandText { get; set; }

        public override int CommandTimeout { get; set; }

        public override CommandType CommandType { get; set; }

        public override UpdateRowSource UpdatedRowSource
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        protected override DbConnection DbConnection { get; set; }

        protected override DbParameterCollection DbParameterCollection { get; }

        protected override DbTransaction DbTransaction { get; set; }

        public override bool DesignTimeVisible
        {
            get => false;
            set => throw new NotSupportedException();
        }

        internal string StatementId { get; set; }
        #endregion

        #region Public Methods
        public override void Prepare()
        {
            throw new NotSupportedException();
        }

        public override int ExecuteNonQuery()
        {
            Task<int> task = ExecuteNonQueryAsync();

            try
            {
                return task.Result;
            }
            catch (AggregateException e)
            {
                var innerException = e.Flatten().InnerException;

                if (innerException != null)
                    throw innerException;

                throw;
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
            Task<object> task = ExecuteScalarAsync(CancellationToken.None);

            try
            {
                return task.Result;
            }
            catch (AggregateException e)
            {
                var innerException = e.Flatten().InnerException;

                if (innerException != null)
                    throw innerException;

                throw;
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
            throw new NotImplementedException();
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            Task<DbDataReader> task = ExecuteDbDataReaderAsync(behavior, CancellationToken.None);

            try
            {
                return task.Result;
            }
            catch (AggregateException e)
            {
                var innerException = e.Flatten().InnerException;

                if (innerException != null)
                    throw innerException;

                throw;
            }
        }

        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            if (!(Connection is JdbcConnection jdbcConnection))
                throw new InvalidOperationException();

            var response = await jdbcConnection.Bridge.Statement.executeStatementAsync(new ExecuteStatementRequest
            {
                StatementId = StatementId,
                Sql = CommandText
            });

            /*
            return new PrestoDataReader(this, response);
            */

            throw new NotImplementedException();
        }

        public override void Cancel()
        {
        }
        #endregion

        #region IDisposable
        protected override void Dispose(bool disposing)
        {
            if (_isDisposed)
                return;

            if (!(Connection is JdbcConnection jdbcConnection))
                throw new InvalidOperationException();

            jdbcConnection.Bridge.Statement.closeStatement(new CloseStatementRequest
            {
                StatementId = StatementId
            });

            _isDisposed = true;

            base.Dispose(disposing);
        }
        #endregion
    }
}
