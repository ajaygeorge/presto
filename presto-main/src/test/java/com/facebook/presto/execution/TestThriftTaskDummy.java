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
package com.facebook.presto.execution;

import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.codec.internal.compiler.CompilerThriftCodecFactory;
import com.facebook.drift.codec.internal.reflection.ReflectionThriftCodecFactory;
import com.facebook.drift.protocol.TBinaryProtocol;
import com.facebook.drift.protocol.TCompactProtocol;
import com.facebook.drift.protocol.TFacebookCompactProtocol;
import com.facebook.drift.protocol.TMemoryBuffer;
import com.facebook.drift.protocol.TProtocol;
import com.facebook.drift.protocol.TTransport;
import com.facebook.presto.testing.TestingMetadataUpdateHandle;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.function.Function;

import static org.testng.Assert.assertEquals;

public class TestThriftTaskDummy
{
    private static final ThriftCodecManager COMPILER_READ_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(true));
    private static final ThriftCodecManager COMPILER_WRITE_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(true));
    private static final ThriftCodec<TaskDummy> COMPILER_READ_CODEC = COMPILER_READ_CODEC_MANAGER.getCodec(TaskDummy.class);
    private static final ThriftCodec<TaskDummy> COMPILER_WRITE_CODEC = COMPILER_WRITE_CODEC_MANAGER.getCodec(TaskDummy.class);
    private static final ThriftCodecManager REFLECTION_READ_CODEC_MANAGER = new ThriftCodecManager(new ReflectionThriftCodecFactory());
    private static final ThriftCodecManager REFLECTION_WRITE_CODEC_MANAGER = new ThriftCodecManager(new ReflectionThriftCodecFactory());
    private static final ThriftCodec<TaskDummy> REFLECTION_READ_CODEC = REFLECTION_READ_CODEC_MANAGER.getCodec(TaskDummy.class);
    private static final ThriftCodec<TaskDummy> REFLECTION_WRITE_CODEC = REFLECTION_WRITE_CODEC_MANAGER.getCodec(TaskDummy.class);
    private static final TMemoryBuffer transport = new TMemoryBuffer(100 * 1024);
    private TaskDummy taskDummy;

    @BeforeMethod
    public void setUp()
    {
        taskDummy = getTaskDummy();
    }

    @DataProvider
    public Object[][] codecCombinations()
    {
        return new Object[][] {
                /*{COMPILER_READ_CODEC, COMPILER_WRITE_CODEC},
                {COMPILER_READ_CODEC, REFLECTION_WRITE_CODEC},
                {REFLECTION_READ_CODEC, COMPILER_WRITE_CODEC},
                */{REFLECTION_READ_CODEC, REFLECTION_WRITE_CODEC}
        };
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeBinaryProtocol(ThriftCodec<TaskDummy> readCodec, ThriftCodec<TaskDummy> writeCodec)
            throws Exception
    {
        TaskDummy taskDummy = getRoundTripSerialize(readCodec, writeCodec, TBinaryProtocol::new);
        assertSerde(taskDummy);
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeTCompactProtocol(ThriftCodec<TaskDummy> readCodec, ThriftCodec<TaskDummy> writeCodec)
            throws Exception
    {
        TaskDummy taskDummy = getRoundTripSerialize(readCodec, writeCodec, TCompactProtocol::new);
        assertSerde(taskDummy);
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeTFacebookCompactProtocol(ThriftCodec<TaskDummy> readCodec, ThriftCodec<TaskDummy> writeCodec)
            throws Exception
    {
        TaskDummy taskDummy = getRoundTripSerialize(readCodec, writeCodec, TFacebookCompactProtocol::new);
        assertSerde(taskDummy);
    }

    private void assertSerde(TaskDummy taskDummy)
    {
        assertEquals(1, taskDummy.getVal());
        TestingMetadataUpdateHandle connectorMetadataUpdateHandle = (TestingMetadataUpdateHandle) taskDummy.getConnectorMetadataUpdateHandle();
        assertEquals(1, connectorMetadataUpdateHandle.getVal());
    }

    private TaskDummy getRoundTripSerialize(ThriftCodec<TaskDummy> readCodec, ThriftCodec<TaskDummy> writeCodec, Function<TTransport, TProtocol> protocolFactory)
            throws Exception
    {
        TProtocol protocol = protocolFactory.apply(transport);
        writeCodec.write(taskDummy, protocol);
        return readCodec.read(protocol);
    }

    private TaskDummy getTaskDummy()
    {
        return new TaskDummy(1, new TestingMetadataUpdateHandle(1));
    }
}
