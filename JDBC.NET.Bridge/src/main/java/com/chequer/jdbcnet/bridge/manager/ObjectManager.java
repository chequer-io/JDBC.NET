package com.chequer.jdbcnet.bridge.manager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class ObjectManager {
    //region Fields
    private static final ConcurrentHashMap<String, PreparedStatement> _statements = new ConcurrentHashMap<String, PreparedStatement>();
    private static final ConcurrentHashMap<String, Connection> _connections = new ConcurrentHashMap<String, Connection>();
    private static final ConcurrentHashMap<String, ResultSet> _resultSets = new ConcurrentHashMap<String, ResultSet>();
    //endregion

    //region Statement Method
    public static String putStatement(PreparedStatement statement) {
        String id = UUID.randomUUID().toString();
        _statements.put(id, statement);

        return id;
    }

    public static PreparedStatement getStatement(String statementId) {
        return _statements.get(statementId);
    }

    public static void removeStatement(String statementId) {
        _statements.remove(statementId);
    }
    //endregion

    //region Connection Method
    public static String putConnection(Connection connection) {
        String id = UUID.randomUUID().toString();
        _connections.put(id, connection);

        return id;
    }

    public static Connection getConnection(String connectionId) {
        return _connections.get(connectionId);
    }

    public static void removeConnection(String connectionId) {
        _connections.remove(connectionId);
    }
    //endregion

    //region ResultSet Method
    public static String putResultSet(ResultSet resultSet) {
        String id = UUID.randomUUID().toString();
        _resultSets.put(id, resultSet);

        return id;
    }

    public static ResultSet getResultSet(String resultSetId) {
        return _resultSets.get(resultSetId);
    }

    public static void removeResultSet(String resultSetId) {
        _resultSets.remove(resultSetId);
    }
    //endregion
}
