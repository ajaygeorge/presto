package com.facebook.presto.ping;

import org.apache.thrift.TException;

import java.util.concurrent.atomic.AtomicInteger;

public class BlockingPingService
        implements PingService
{
    private final AtomicInteger counter = new AtomicInteger();

    @Override
    public PingResponse ping(PingRequest pingRequest)
            throws TException
    {
        return new PingResponse.Builder()
                .setResponse(pingRequest.getRequest() + " Pong " + counter.getAndIncrement())
                .build();
    }

    @Override
    public void close()
    {
    }
}
