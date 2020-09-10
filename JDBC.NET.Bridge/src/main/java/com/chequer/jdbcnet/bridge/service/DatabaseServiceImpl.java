package com.chequer.jdbcnet.bridge.service;

import com.chequer.jdbc.net.database.Database;
import com.chequer.jdbc.net.database.DatabaseServiceGrpc;
import io.grpc.stub.StreamObserver;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class DatabaseServiceImpl extends DatabaseServiceGrpc.DatabaseServiceImplBase {
    private final ConcurrentHashMap<String, java.sql.Connection> _connections = new ConcurrentHashMap<String, java.sql.Connection>();

    @Override
    public void openConnection(Database.OpenConnectionRequest request, StreamObserver<Database.OpenConnectionResponse> responseObserver) {
        try {
            String id = UUID.randomUUID().toString();
            Connection connection = DriverManager.getConnection(request.getJdbcUrl());
            _connections.put(id, connection);

            DatabaseMetaData metaData = connection.getMetaData();

            Database.OpenConnectionResponse response = Database.OpenConnectionResponse.newBuilder()
                    .setId(id)
                    .setCatalog(connection.getCatalog())
                    .setDatabaseMajorVersion(metaData.getDatabaseMajorVersion())
                    .setDatabaseMinorVersion(metaData.getDatabaseMinorVersion())
                    .setDatabaseProductVersion(metaData.getDatabaseProductVersion())
                    .setDatabaseProductName(metaData.getDatabaseProductName())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void closeConnection(Database.CloseConnectionRequest request, StreamObserver<Database.CloseConnectionResponse> responseObserver) {
        try {
            if (!_connections.containsKey(request.getId())) {
                responseObserver.onError(new Exception("Connection could not be found."));
                return;
            }

            Connection connection = _connections.get(request.getId());
            connection.close();

            _connections.remove(request.getId());

            Database.CloseConnectionResponse response = Database.CloseConnectionResponse.newBuilder()
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
