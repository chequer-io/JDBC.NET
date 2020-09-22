package com.chequer.jdbcnet.bridge.service;

import com.chequer.jdbcnet.bridge.manager.ObjectManager;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import proto.database.Database;
import proto.database.DatabaseServiceGrpc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;

public class DatabaseServiceImpl extends DatabaseServiceGrpc.DatabaseServiceImplBase {
    @Override
    public void openConnection(Database.OpenConnectionRequest request, StreamObserver<Database.OpenConnectionResponse> responseObserver) {
        try {
            Connection connection = DriverManager.getConnection(request.getJdbcUrl());
            connection.setAutoCommit(false);

            String connectionId = ObjectManager.putConnection(connection);

            DatabaseMetaData metaData = connection.getMetaData();

            Database.OpenConnectionResponse response = Database.OpenConnectionResponse.newBuilder()
                    .setConnectionId(connectionId)
                    .setCatalog(connection.getCatalog())
                    .setDatabaseMajorVersion(metaData.getDatabaseMajorVersion())
                    .setDatabaseMinorVersion(metaData.getDatabaseMinorVersion())
                    .setDatabaseProductVersion(metaData.getDatabaseProductVersion())
                    .setDatabaseProductName(metaData.getDatabaseProductName())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void closeConnection(Database.CloseConnectionRequest request, StreamObserver<Empty> responseObserver) {
        try {
            Connection connection = ObjectManager.getConnection(request.getConnectionId());
            connection.close();

            ObjectManager.removeConnection(request.getConnectionId());

            Empty response = Empty.newBuilder()
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void changeCatalog(Database.ChangeCatalogRequest request, StreamObserver<Empty> responseObserver) {
        try {
            Connection connection = ObjectManager.getConnection(request.getConnectionId());
            connection.setCatalog(request.getCatalogName());

            Empty response = Empty.newBuilder()
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }
}
