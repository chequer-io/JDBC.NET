package com.chequer.jdbcnet.bridge.manager;

import com.chequer.jdbcnet.bridge.models.ResultSetEx;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class ObjectManager {
    //region Fields
    private static final ConcurrentHashMap<String, Statement> _statements = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Connection> _connections = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, ResultSetEx> _resultSets = new ConcurrentHashMap<>();
    //endregion

    //region Statement Method
    public static String putStatement(Statement statement) {
        var id = UUID.randomUUID().toString();
        _statements.put(id, statement);

        return id;
    }

    public static Statement getStatement(String statementId) {
        return _statements.get(statementId);
    }

    public static void removeStatement(String statementId) {
        _statements.remove(statementId);
    }
    //endregion

    //region Connection Method
    public static String putConnection(Connection connection) {
        var id = UUID.randomUUID().toString();
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
    public static String putResultSet(ResultSetEx resultSetEx) {
        var id = UUID.randomUUID().toString();
        _resultSets.put(id, resultSetEx);

        return id;
    }

    public static ResultSetEx getResultSet(String resultSetExId) {
        return _resultSets.get(resultSetExId);
    }

    public static void removeResultSet(String resultSetExId) {
        _resultSets.remove(resultSetExId);
    }
    //endregion
}
