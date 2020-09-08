package com.chequer.jdbcnet.bridge.service;

import bridge.Bridge;
import bridge.BridgeServiceGrpc;
import io.grpc.stub.StreamObserver;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Driver;

public class BridgeServiceImpl extends BridgeServiceGrpc.BridgeServiceImplBase {
    @Override
    public void loadDriver(Bridge.LoadDriverRequest request, StreamObserver<Bridge.LoadDriverResponse> responseObserver) {
        try {
            // Load JAR from path
            File file = new File(request.getPath());
            URLClassLoader classLoader = (URLClassLoader)ClassLoader.getSystemClassLoader();
            Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
            method.setAccessible(true);
            method.invoke(classLoader, file.toURI().toURL());

            // Load Class from driver
            Class clazz = Class.forName(request.getClassName());
            Driver driver = (Driver)clazz.newInstance();

            // Return response
            Bridge.LoadDriverResponse response = Bridge.LoadDriverResponse.newBuilder()
                    .setMajorVersion(driver.getMajorVersion())
                    .setMinorVersion(driver.getMinorVersion())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
