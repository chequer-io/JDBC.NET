package com.chequer.jdbcnet.bridge;

import com.chequer.jdbcnet.bridge.service.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class Main {
    private static Server server;

    private static int start(String bridgeId, int bridgeHostPort) throws IOException {
        server = ServerBuilder.forPort(0)
                .addService(new DriverServiceImpl())
                .addService(new DatabaseServiceImpl())
                .addService(new StatementServiceImpl())
                .addService(new MetaDataServiceImpl())
                .addService(new ReaderServiceImpl())
                .build()
                .start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (server != null) {
                    server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
        }));

        var bridePort = server.getPort();

        var data = ByteBuffer.allocate(12);
        data.put(bridgeId.getBytes(StandardCharsets.US_ASCII));
        data.put((byte) (bridePort & 0xff));
        data.put((byte) ((bridePort >> 8) & 0xff));
        data.flip();

        var socketChannel = SocketChannel.open();
        socketChannel.connect(new InetSocketAddress(bridgeHostPort));
        socketChannel.write(data);
        socketChannel.close();

        return bridePort;
    }

    private static void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) {
        var options = new Options();

        var bridgeIdOption = new Option("i", "id", true, "Specifies the bridge id.");
        var bridgeHostPortOption = new Option("p", "port", true, "Specifies the bridge host port.");

        bridgeIdOption.setRequired(true);
        bridgeHostPortOption.setRequired(true);

        options.addOption(bridgeIdOption);
        options.addOption(bridgeHostPortOption);

        var parser = new DefaultParser();

        try {
            var cmd = parser.parse(options, args);
            var bridgeId = cmd.getOptionValue("id");
            var bridgeHostPort = Integer.parseInt(cmd.getOptionValue("port"));

            var bridgePort = start(bridgeId, bridgeHostPort);
            System.out.println("JDBC.NET.Bridge is running on port " + bridgePort + "...");

            blockUntilShutdown();
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            System.exit(1);
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }
}
