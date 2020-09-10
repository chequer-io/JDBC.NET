package com.chequer.jdbcnet.bridge.service;

import io.grpc.stub.StreamObserver;
import proto.Common;
import proto.database.Database;
import proto.database.DatabaseServiceGrpc;

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
                    .setConnectionId(id)
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
    public void closeConnection(Database.CloseConnectionRequest request, StreamObserver<Common.Empty> responseObserver) {
        try {
            if (!_connections.containsKey(request.getConnectionId())) {
                responseObserver.onError(new Exception("Connection could not be found."));
                return;
            }

            Connection connection = _connections.get(request.getConnectionId());
            connection.close();

            _connections.remove(request.getConnectionId());

            Common.Empty response = Common.Empty.newBuilder()
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void changeCatalog(Database.ChangeCatalogRequest request, StreamObserver<Common.Empty> responseObserver) {
        try {
            if (!_connections.containsKey(request.getConnectionId())) {
                responseObserver.onError(new Exception("Connection could not be found."));
                return;
            }

            Connection connection = _connections.get(request.getConnectionId());
            connection.setCatalog(request.getCatalogName());

            Common.Empty response = Common.Empty.newBuilder()
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
