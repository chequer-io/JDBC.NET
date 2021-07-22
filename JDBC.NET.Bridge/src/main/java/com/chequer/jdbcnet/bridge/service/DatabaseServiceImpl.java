package com.chequer.jdbcnet.bridge.service;

import com.chequer.jdbcnet.bridge.manager.ObjectManager;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import proto.database.Database;
import proto.database.DatabaseServiceGrpc;

import java.sql.DriverManager;
import java.util.Optional;
import java.util.Properties;

public class DatabaseServiceImpl extends DatabaseServiceGrpc.DatabaseServiceImplBase {
    @Override
    public void openConnection(Database.OpenConnectionRequest request, StreamObserver<Database.OpenConnectionResponse> responseObserver) {
        try {
            var properties = new Properties();
            properties.putAll(request.getPropertiesMap());

            var connection = DriverManager.getConnection(request.getJdbcUrl(), properties);
            connection.setAutoCommit(true);

            var connectionId = ObjectManager.putConnection(connection);
            var metaData = connection.getMetaData();

            var response = Database.OpenConnectionResponse.newBuilder()
                    .setConnectionId(connectionId)
                    .setCatalog(Optional.ofNullable(connection.getCatalog()).orElse(""))
                    .setDatabaseMajorVersion(metaData.getDatabaseMajorVersion())
                    .setDatabaseMinorVersion(metaData.getDatabaseMinorVersion())
                    .setDatabaseProductVersion(Optional.ofNullable(metaData.getDatabaseProductVersion()).orElse(""))
                    .setDatabaseProductName(Optional.ofNullable(metaData.getDatabaseProductName()).orElse(""))
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Throwable e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void closeConnection(Database.CloseConnectionRequest request, StreamObserver<Empty> responseObserver) {
        try {
            var connection = ObjectManager.getConnection(request.getConnectionId());
            connection.close();

            ObjectManager.removeConnection(request.getConnectionId());

            var response = Empty.newBuilder()
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Throwable e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void changeCatalog(Database.ChangeCatalogRequest request, StreamObserver<Empty> responseObserver) {
        try {
            var connection = ObjectManager.getConnection(request.getConnectionId());
            connection.setCatalog(request.getCatalogName());

            var response = Empty.newBuilder()
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Throwable e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void setAutoCommit(Database.SetAutoCommitRequest request, StreamObserver<Empty> responseObserver) {
        try {
            var connection = ObjectManager.getConnection(request.getConnectionId());
            connection.setAutoCommit(request.getUseAutoCommit());

            var response = Empty.newBuilder()
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Throwable e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void getTransactionIsolation(Database.GetTransactionIsolationRequest request, StreamObserver<Database.GetTransactionIsolationResponse> responseObserver) {
        try {
            var connection = ObjectManager.getConnection(request.getConnectionId());

            var response = Database.GetTransactionIsolationResponse.newBuilder()
                    .setIsolationValue(connection.getTransactionIsolation())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Throwable e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    @SuppressWarnings("MagicConstant")
    public void setTransactionIsolation(Database.SetTransactionIsolationRequest request, StreamObserver<Empty> responseObserver) {
        try {
            var connection = ObjectManager.getConnection(request.getConnectionId());
            connection.setTransactionIsolation(request.getIsolationValue());

            var response = Empty.newBuilder()
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Throwable e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void rollback(Database.TransactionRequest request, StreamObserver<Empty> responseObserver) {
        try {
            var connection = ObjectManager.getConnection(request.getConnectionId());
            connection.rollback();

            var response = Empty.newBuilder()
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Throwable e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void commit(Database.TransactionRequest request, StreamObserver<Empty> responseObserver) {
        try {
            var connection = ObjectManager.getConnection(request.getConnectionId());
            connection.commit();

            var response = Empty.newBuilder()
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Throwable e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }
}
