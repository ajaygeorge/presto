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
import com.facebook.drift.codec.metadata.Any;
import com.facebook.drift.transport.netty.codec.Protocol;
import com.facebook.presto.spi.ConnectorMetadataUpdateHandle;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class ConnectorSerde
{
    private final HandleResolver handleResolver;
    private final ThriftCodec<Any> codec;

    public ConnectorSerde(HandleResolver handleResolver)
    {
        this.handleResolver = handleResolver;
        this.codec = new AnyThriftCodec<>(
                handleResolver::getId,
                handleResolver::getMetadataUpdateHandleClass);
    }

    public byte[] serialize(ConnectorMetadataUpdateHandle connectorMetadataUpdateHandle)
    {
        //id from handleResolver
        String id = handleResolver.getId(connectorMetadataUpdateHandle);
        Any packedObj = Any.pack(connectorMetadataUpdateHandle, id);

        //ByteBuffer buf = ByteBuffer.allocate(102400);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try {
            //ThriftProtocolUtils.write(packedObj, codec, Protocol.BINARY, new ByteBufferBackedOutputStream(buf));
            ThriftProtocolUtils.write(packedObj, codec, Protocol.BINARY, byteArrayOutputStream);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return byteArrayOutputStream.toByteArray();
    }

    public ConnectorMetadataUpdateHandle deSerialize(byte[] byteBuffer)
    {
        try {
            Any read = ThriftProtocolUtils.read(codec, Protocol.BINARY, new ByteArrayInputStream(byteBuffer));
            Class<? extends ConnectorMetadataUpdateHandle> metadataUpdateHandleClass = handleResolver.getMetadataUpdateHandleClass(read.getId());
            return metadataUpdateHandleClass.cast(read.getObj());
        }
        catch (ThriftProtocolException e) {
            e.printStackTrace();
        }

        return null;
    }
}
