package com.facebook.presto.server.thrift;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;

import java.util.Arrays;

@ThriftStruct
public class Any
{
    private String id;
    private byte[] bytes;

    @ThriftConstructor
    public Any(String id, byte[] bytes)
    {
        this.id = id;
        this.bytes = Arrays.copyOf(bytes, bytes.length);
    }

    @ThriftField(1)
    public String getId()
    {
        return id;
    }

    @ThriftField(2)
    public byte[] getBytes()
    {
        //TODO : Remove copying. Temporarily for satisyfing spotbugs
        return Arrays.copyOf(bytes, bytes.length);
    }
}

