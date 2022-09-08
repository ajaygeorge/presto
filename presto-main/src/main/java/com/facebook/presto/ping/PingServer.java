package com.facebook.presto.ping;

import com.facebook.swift.service.ThriftServerConfig;
import com.facebook.thrift.server.RpcServerHandler;
import com.facebook.thrift.server.ServerTransport;
import com.facebook.thrift.util.RpcServerUtils;
import com.facebook.thrift.util.TransportType;

public class PingServer
{
    public void start()
    {
        RpcServerHandler handler = PingService.serverHandlerBuilder(new BlockingPingService()).build();

        ServerTransport transport =
                RpcServerUtils.createServerTransport(
                                new ThriftServerConfig()
                                        .setPort(7777)
                                        .setEnableJdkSsl(true), //With Open SSL we see tcnative class not found errors.
                                TransportType.RSOCKET,
                                handler)
                        .block();

        System.out.println("server started at -> " + transport.getAddress());
        transport.onClose().block();
    }

    public static void main(String[] args)
    {
        new PingServer().start();
    }
}
