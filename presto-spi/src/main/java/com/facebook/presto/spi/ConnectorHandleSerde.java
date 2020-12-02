package com.facebook.presto.spi;

public interface ConnectorHandleSerde<T>
{
    byte[] serialize(T val);

    T deSerialize(String id, byte[] byteArr);
}
