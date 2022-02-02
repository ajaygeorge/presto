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

import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.codec.internal.ProtocolReader;
import com.facebook.drift.codec.internal.ProtocolWriter;
import com.facebook.drift.codec.internal.builtin.ByteBufferThriftCodec;
import com.facebook.drift.codec.internal.builtin.StringThriftCodec;
import com.facebook.drift.codec.internal.compiler.CompilerThriftCodecFactory;
import com.facebook.drift.codec.metadata.Any;
import com.facebook.drift.codec.metadata.ThriftType;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;

import java.nio.ByteBuffer;
import java.util.function.Function;

public class AnyThriftCodec
        implements ThriftCodec<Any>
{

    @Override
    public ThriftType getType()
    {
        return ThriftType.BINARY;
    }

    @Override
    public Any read(TProtocolReader protocol)
            throws Exception
    {
        String id = null;
        byte[] byteArr = new byte[]{};
        ProtocolReader protocolReader = new ProtocolReader(protocol);
        protocolReader.readStructBegin();
        while (protocolReader.nextField()) {
            if (protocolReader.getFieldId() == 1) {
                id = protocolReader.readStringField();
            }
            else if (protocolReader.getFieldId() == 2) {
                byteArr = protocolReader.readBinaryField().array();
            }
        }
        protocolReader.readStructEnd();
        return new Any(id, byteArr);
    }

    @Override
    public void write(Any value, TProtocolWriter protocol)
            throws Exception
    {
        ProtocolWriter writer = new ProtocolWriter(protocol);
        writer.writeStructBegin("Any");
        String id = value.getId();
        writer.writeStringField("id", (short) 1, id);
        byte[] bytes = value.getBytes();
        writer.writeBinaryField("byteArr", (short) 2, ByteBuffer.wrap(bytes));
        writer.writeStructEnd();
    }
}
