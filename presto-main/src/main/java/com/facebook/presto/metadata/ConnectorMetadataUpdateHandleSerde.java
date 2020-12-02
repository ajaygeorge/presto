/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.metadata;

import com.facebook.airlift.http.client.thrift.ThriftProtocolException;
import com.facebook.airlift.http.client.thrift.ThriftProtocolUtils;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.codec.internal.compiler.CompilerThriftCodecFactory;
import com.facebook.drift.transport.netty.codec.Protocol;
import com.facebook.presto.spi.ConnectorHandleSerde;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.function.Function;

public class ConnectorMetadataUpdateHandleSerde<T> implements ConnectorHandleSerde<T>
{
    private final Function<T, String> nameResolver;
    private final Function<String, Class<? extends T>> classResolver;

    public ConnectorMetadataUpdateHandleSerde(
            Function<T, String> nameResolver,
            Function<String, Class<? extends T>> classResolver)
    {
        this.nameResolver = nameResolver;
        this.classResolver = classResolver;
    }

    public byte[] serialize(T val)
    {
        ThriftCodec codec = new ThriftCodecManager(new CompilerThriftCodecFactory(true)).getCodec(val.getClass());
        ByteArrayOutputStream connectorSerializedOutputStream = new ByteArrayOutputStream();
        try {
            ThriftProtocolUtils.write(val, codec, Protocol.BINARY, connectorSerializedOutputStream);
        }
        catch (Exception e1) {
            e1.printStackTrace();
        }
        return connectorSerializedOutputStream.toByteArray();
    }

    public T deSerialize(String id, byte[] byteBuffer)
    {
        try {
            Class<? extends T> aClass = classResolver.apply(id);
            ThriftCodec<? extends T> codec = new ThriftCodecManager(new CompilerThriftCodecFactory(true)).getCodec(aClass);
            return ThriftProtocolUtils.read(codec, Protocol.BINARY, new ByteArrayInputStream(byteBuffer));
        }
        catch (ThriftProtocolException e) {
            e.printStackTrace();
        }

        return null;
    }
}
