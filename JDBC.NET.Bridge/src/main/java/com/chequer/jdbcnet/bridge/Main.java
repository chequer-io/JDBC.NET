package com.chequer.jdbcnet.bridge;

import com.chequer.jdbcnet.bridge.service.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.grpc.netty.shaded.io.netty.channel.unix.DomainSocketAddress;
import io.netty.util.internal.StringUtil;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class Main {
    private static Server server;

    private static void start(int port, String name) throws IOException {
        server = createServer(port, name).start();

        if (port <= 0) {
            System.out.println("JDBC.NET.Bridge is running on " + name);
        } else {
            System.out.println("JDBC.NET.Bridge is running on port " + port + "...");
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (server != null) {
                    server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
        }));
    }

    private static Server createServer(int port, String name) {
        EpollEventLoopGroup group = port <= 0 ? new EpollEventLoopGroup() : null;

        var builder = group == null
                ? ServerBuilder.forPort(port)
                : NettyServerBuilder.forAddress(new DomainSocketAddress(name))
                .channelType(EpollServerDomainSocketChannel.class)
                .workerEventLoopGroup(group)
                .bossEventLoopGroup(group);

        return builder.addService(new DriverServiceImpl())
                .addService(new DatabaseServiceImpl())
                .addService(new StatementServiceImpl())
                .addService(new MetaDataServiceImpl())
                .addService(new ReaderServiceImpl())
                .build();
    }

    private static void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) {
        var options = new Options();
        options.addOption(new Option("p", "port", true, "Specifies the port to open."));
        options.addOption(new Option("n", "name", true, "Specifies the name to open."));

        var parser = new DefaultParser();

        try {
            var cmd = parser.parse(options, args);
            var port = cmd.getOptionValue("port");
            var name = cmd.getOptionValue("name");

            start(!StringUtil.isNullOrEmpty(port) ? Integer.parseInt(port) : 0, name);
            blockUntilShutdown();
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            System.exit(1);
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }
}
