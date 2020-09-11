package com.chequer.jdbcnet.bridge.manager;

import java.sql.Connection;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class ObjectManager {
    //region Fields
    private static final ConcurrentHashMap<String, Statement> _statements = new ConcurrentHashMap<String, Statement>();
    private static final ConcurrentHashMap<String, Connection> _connections = new ConcurrentHashMap<String, Connection>();
    //endregion

    //region Statement Method
    public static String putStatement(Statement statement) {
        String id = UUID.randomUUID().toString();
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
}
