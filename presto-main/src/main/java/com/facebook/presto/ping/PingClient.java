package com.facebook.presto.ping;

import com.facebook.thrift.client.RpcClientFactory;
import com.facebook.thrift.client.ThriftClientConfig;
import com.facebook.thrift.rsocket.client.RSocketRpcClientFactory;
import org.apache.thrift.ProtocolId;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class PingClient
{
    public void start(String host, int port)
    {
        RpcClientFactory clientFactory = new RSocketRpcClientFactory(new ThriftClientConfig().setEnableJdkSsl(true));
        SocketAddress address = InetSocketAddress.createUnresolved(host, port);

        final PingService client =
                PingService.clientBuilder()
                        .setProtocolId(ProtocolId.BINARY)
                        .build(clientFactory, address);

        PingRequest request = new PingRequest("Ping");
        PingResponse response = client.ping(request);
        System.out.println(response.getResponse());
    }

    public static void main(String[] args)
    {
        new PingClient().start("127.0.0.1", 7777);
    }
}
