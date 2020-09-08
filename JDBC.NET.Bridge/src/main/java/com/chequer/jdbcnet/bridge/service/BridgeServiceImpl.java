package com.chequer.jdbcnet.bridge.service;

import bridge.Bridge;
import bridge.BridgeServiceGrpc;
import io.grpc.stub.StreamObserver;

public class BridgeServiceImpl extends BridgeServiceGrpc.BridgeServiceImplBase {
    @Override
    public void loadDriver(Bridge.LoadDriverRequest request, StreamObserver<Bridge.LoadDriverResponse> responseObserver) {

        Bridge.LoadDriverResponse response = Bridge.LoadDriverResponse.newBuilder()
                .setVersion("1.2.0")
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
