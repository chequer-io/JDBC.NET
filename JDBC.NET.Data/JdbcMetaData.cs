using System;
using JDBC.NET.Data.Exceptions;
using JDBC.NET.Proto;

namespace JDBC.NET.Data
{
    /// <summary>
    /// https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.htm
    /// </summary>
    public class JdbcMetaData
    {
        #region Properties
        public JdbcConnection Connection { get; }

        public JdbcCommand EmptyCommand { get; }
        #endregion

        #region Constructor
        internal JdbcMetaData(JdbcConnection connection)
        {
            Connection = connection;
            EmptyCommand = new JdbcEmptyCommand(connection);
        }
        #endregion

        #region Private Methods
        private GetMetaDataRequest CreateRequest()
        {
            return new()
            {
                ConnectionId = Connection.ConnectionId
            };
        }
        #endregion

        #region ResultSet Methods
        public JdbcDataReader GetTables(string catalog = null, string schemaPattern = null, string tableNamePattern = null, string[] types = null)
        {
            Connection.CheckOpen();

            var response = Connection.Bridge.MetaData.getTables(new GetTablesRequest
            {
                ConnectionId = Connection.ConnectionId,
                Catalog = catalog ?? string.Empty,
                SchemaPattern = schemaPattern ?? string.Empty,
                TableNamePattern = tableNamePattern ?? string.Empty,
                Types_ = { types ?? Array.Empty<string>() }
            });

            return new JdbcDataReader(EmptyCommand, response);
        }

        public JdbcDataReader GetCatalogs()
        {
            Connection.CheckOpen();

            var response = Connection.Bridge.MetaData.getCatalogs(CreateRequest());
            return new JdbcDataReader(EmptyCommand, response);
        }

        public JdbcDataReader GetProcedures(string catalog = null, string schemaPattern = null, string procedureNamePattern = null)
        {
            Connection.CheckOpen();

            var response = Connection.Bridge.MetaData.getProcedures(new GetProceduresRequest
            {
                ConnectionId = Connection.ConnectionId,
                Catalog = catalog ?? string.Empty,
                SchemaPattern = schemaPattern ?? string.Empty,
                ProcedureNamePattern = procedureNamePattern ?? string.Empty
            });

            return new JdbcDataReader(EmptyCommand, response);
        }

        public JdbcDataReader GetFunctions(string catalog = null, string schemaPattern = null, string functionNamePattern = null)
        {
            Connection.CheckOpen();

            var response = Connection.Bridge.MetaData.getFunctions(new GetFunctionsRequest
            {
                ConnectionId = Connection.ConnectionId,
                Catalog = catalog ?? string.Empty,
                SchemaPattern = schemaPattern ?? string.Empty,
                FunctionNamePattern = functionNamePattern ?? string.Empty
            });

            return new JdbcDataReader(EmptyCommand, response);
        }
        #endregion

        #region Boolean Methods
        public bool IsReadOnly()
        {
            Connection.CheckOpen();
            return Connection.Bridge.MetaData.isReadOnly(CreateRequest()).Value;
        }

        public bool IsSupportsGroupBy()
        {
            Connection.CheckOpen();
            return Connection.Bridge.MetaData.supportsGroupBy(CreateRequest()).Value;
        }
        #endregion
    }
}
