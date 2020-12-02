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

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.metadata.ConnectorSerde;
import com.facebook.presto.spi.ConnectorMetadataUpdateHandle;

@ThriftStruct
public class TaskDummy
{
    private final int val;
    private ConnectorMetadataUpdateHandle connectorMetadataUpdateHandle;

    public void setConnectorSerde(ConnectorSerde connectorSerde)
    {
        this.connectorSerde = connectorSerde;
    }

    private ConnectorSerde connectorSerde;
    private final byte[] connectorMetadataUpdateHandleByteBuffer;

    public TaskDummy(
            int val,
            ConnectorMetadataUpdateHandle connectorMetadataUpdateHandle,
            ConnectorSerde connectorSerde)
    {
        this.val = val;
        this.connectorSerde = connectorSerde;
        this.connectorMetadataUpdateHandle = connectorMetadataUpdateHandle;
        this.connectorMetadataUpdateHandleByteBuffer = connectorSerde.serialize(connectorMetadataUpdateHandle);
    }

    @ThriftConstructor
    public TaskDummy(
            int val,
            byte[] connectorMetadataUpdateHandleByteBuffer)
    {
        this.val = val;
        this.connectorMetadataUpdateHandleByteBuffer = connectorMetadataUpdateHandleByteBuffer;
    }

    @ThriftField(1)
    public int getVal()
    {
        return val;
    }

    @ThriftField(2)
    public byte[] getConnectorMetadataUpdateHandleByteBuffer()
    {
        return connectorMetadataUpdateHandleByteBuffer;
    }

    public ConnectorMetadataUpdateHandle getConnectorMetadataUpdateHandle()
    {
        return connectorSerde.deSerialize(connectorMetadataUpdateHandleByteBuffer);
    }
}
