package com.chequer.jdbcnet.bridge.service;

import com.chequer.jdbcnet.bridge.manager.ObjectManager;
import com.chequer.jdbcnet.bridge.models.ResultSetEx;
import com.chequer.jdbcnet.bridge.utils.Utils;
import com.google.common.base.Strings;
import com.google.protobuf.BoolValue;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import proto.Common;
import proto.metadata.MetaDataServiceGrpc;
import proto.metadata.Metadata;

public class MetaDataServiceImpl extends MetaDataServiceGrpc.MetaDataServiceImplBase {
    //region ResultSet Method
    @Override
    public void getTables(Metadata.GetTablesRequest request, StreamObserver<Common.JdbcResultSetResponse> responseObserver) {
        try {
            var metaData = ObjectManager.getConnection(request.getConnectionId()).getMetaData();

            var resultSet = new ResultSetEx(metaData.getTables(
                    Strings.emptyToNull(request.getCatalog()),
                    Strings.emptyToNull(request.getSchemaPattern()),
                    Strings.emptyToNull(request.getTableNamePattern()),
                    Utils.emptyArrayToNull(request.getTypesList().toArray(new String[0]))));

            var resultSetId = ObjectManager.putResultSet(resultSet);

            var responseBuilder = Common.JdbcResultSetResponse.newBuilder()
                    .setResultSetId(resultSetId)
                    .setHasRows(resultSet.getHasRows());

            Utils.addColumns(responseBuilder, resultSet.getMetaData());
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Throwable e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void getCatalogs(Metadata.GetMetaDataRequest request, StreamObserver<Common.JdbcResultSetResponse> responseObserver) {
        try {
            var metaData = ObjectManager.getConnection(request.getConnectionId()).getMetaData();

            var resultSet = new ResultSetEx(metaData.getCatalogs());
            var resultSetId = ObjectManager.putResultSet(resultSet);

            var responseBuilder = Common.JdbcResultSetResponse.newBuilder()
                    .setResultSetId(resultSetId)
                    .setHasRows(resultSet.getHasRows());

            Utils.addColumns(responseBuilder, resultSet.getMetaData());
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Throwable e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void getProcedures(Metadata.GetProceduresRequest request, StreamObserver<Common.JdbcResultSetResponse> responseObserver) {
        try {
            var metaData = ObjectManager.getConnection(request.getConnectionId()).getMetaData();

            var resultSet = new ResultSetEx(metaData.getProcedures(
                    Strings.emptyToNull(request.getCatalog()),
                    Strings.emptyToNull(request.getSchemaPattern()),
                    Strings.emptyToNull(request.getProcedureNamePattern())));

            var resultSetId = ObjectManager.putResultSet(resultSet);

            var responseBuilder = Common.JdbcResultSetResponse.newBuilder()
                    .setResultSetId(resultSetId)
                    .setHasRows(resultSet.getHasRows());

            Utils.addColumns(responseBuilder, resultSet.getMetaData());
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Throwable e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void getFunctions(Metadata.GetFunctionsRequest request, StreamObserver<Common.JdbcResultSetResponse> responseObserver) {
        try {
            var metaData = ObjectManager.getConnection(request.getConnectionId()).getMetaData();

            var resultSet = new ResultSetEx(metaData.getFunctions(
                    Strings.emptyToNull(request.getCatalog()),
                    Strings.emptyToNull(request.getSchemaPattern()),
                    Strings.emptyToNull(request.getFunctionNamePattern())));

            var resultSetId = ObjectManager.putResultSet(resultSet);

            var responseBuilder = Common.JdbcResultSetResponse.newBuilder()
                    .setResultSetId(resultSetId)
                    .setHasRows(resultSet.getHasRows());

            Utils.addColumns(responseBuilder, resultSet.getMetaData());
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Throwable e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }
    //endregion

    //region Boolean Method
    @Override
    public void isReadOnly(Metadata.GetMetaDataRequest request, StreamObserver<BoolValue> responseObserver) {
        try {
            var metaData = ObjectManager.getConnection(request.getConnectionId()).getMetaData();

            var response = BoolValue.newBuilder()
                    .setValue(metaData.isReadOnly())
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
    public void supportsGroupBy(Metadata.GetMetaDataRequest request, StreamObserver<BoolValue> responseObserver) {
        try {
            var metaData = ObjectManager.getConnection(request.getConnectionId()).getMetaData();

            var response = BoolValue.newBuilder()
                    .setValue(metaData.supportsGroupBy())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Throwable e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }
    //endregion
}
