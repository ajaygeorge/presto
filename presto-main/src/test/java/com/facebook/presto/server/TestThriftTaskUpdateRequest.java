package com.facebook.presto.server;

import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.codec.internal.compiler.CompilerThriftCodecFactory;
import com.facebook.drift.codec.internal.reflection.ReflectionThriftCodecFactory;
import com.facebook.drift.protocol.TBinaryProtocol;
import com.facebook.drift.protocol.TMemoryBuffer;
import com.facebook.drift.protocol.TProtocol;
import com.facebook.drift.protocol.TTransport;
import com.facebook.presto.SessionRepresentation;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.ExecutionWriterTarget;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.metadata.AnalyzeTableHandle;
import com.facebook.presto.metadata.OutputTableHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.session.ResourceEstimates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.sql.planner.iterative.rule.test.RuleTester.CONNECTOR_ID;

public class TestThriftTaskUpdateRequest
{
    private static final ThriftCodecManager COMPILER_READ_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false));
    private static final ThriftCodecManager COMPILER_WRITE_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false));
    private static final ThriftCodec<TaskUpdateRequest> COMPILER_READ_CODEC = COMPILER_READ_CODEC_MANAGER.getCodec(TaskUpdateRequest.class);
    private static final ThriftCodec<TaskUpdateRequest> COMPILER_WRITE_CODEC = COMPILER_WRITE_CODEC_MANAGER.getCodec(TaskUpdateRequest.class);
    private static final ThriftCodecManager REFLECTION_READ_CODEC_MANAGER = new ThriftCodecManager(new ReflectionThriftCodecFactory());
    private static final ThriftCodecManager REFLECTION_WRITE_CODEC_MANAGER = new ThriftCodecManager(new ReflectionThriftCodecFactory());
    private static final ThriftCodec<TaskUpdateRequest> REFLECTION_READ_CODEC = REFLECTION_READ_CODEC_MANAGER.getCodec(TaskUpdateRequest.class);
    private static final ThriftCodec<TaskUpdateRequest> REFLECTION_WRITE_CODEC = REFLECTION_WRITE_CODEC_MANAGER.getCodec(TaskUpdateRequest.class);
    private static final TMemoryBuffer transport = new TMemoryBuffer(100 * 1024);
    private TaskUpdateRequest taskUpdateRequest;

    @BeforeMethod
    public void setUp()
    {
        taskUpdateRequest = getTaskUpdateRequest();
    }

    @DataProvider
    public Object[][] codecCombinations()
    {
        return new Object[][] {
                {COMPILER_READ_CODEC, COMPILER_WRITE_CODEC},
                {COMPILER_READ_CODEC, REFLECTION_WRITE_CODEC},
                {REFLECTION_READ_CODEC, COMPILER_WRITE_CODEC},
                {REFLECTION_READ_CODEC, REFLECTION_WRITE_CODEC}
        };
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeBinaryProtocol(ThriftCodec<TaskUpdateRequest> readCodec, ThriftCodec<TaskUpdateRequest> writeCodec)
            throws Exception
    {
        TaskUpdateRequest taskUpdateRequest = getRoundTripSerialize(readCodec, writeCodec, TBinaryProtocol::new);
        assertSerde(taskUpdateRequest);
    }

    private void assertSerde(TaskUpdateRequest taskUpdateRequest)
    {
        taskUpdateRequest.getSources();
    }

    private TaskUpdateRequest getRoundTripSerialize(ThriftCodec<TaskUpdateRequest> readCodec, ThriftCodec<TaskUpdateRequest> writeCodec, Function<TTransport, TProtocol> protocolFactory)
            throws Exception
    {
        TProtocol protocol = protocolFactory.apply(transport);
        writeCodec.write(taskUpdateRequest, protocol);
        return readCodec.read(protocol);
    }

    private TaskUpdateRequest getTaskUpdateRequest()
    {
        SessionRepresentation session = getSessionRepresentation();
        Map<String, String> extraCredentials = getExtraCredentials();
        Optional<byte[]> fragment = Optional.of("Hello".getBytes());
        List<TaskSource> sources = getSources();
        OutputBuffers outputIds = getOutputIds();
        Optional<TableWriteInfo> tableWriteInfo = Optional.empty(); //getTableWriteInfo();
        return new TaskUpdateRequest(session, extraCredentials, fragment, sources, outputIds, tableWriteInfo);
    }

    private Optional<TableWriteInfo> getTableWriteInfo()
    {
        ExecutionWriterTarget.CreateHandle createHandle = new ExecutionWriterTarget.CreateHandle(new OutputTableHandle(
                CONNECTOR_ID,
                new ConnectorTransactionHandle() {},
                new ConnectorOutputTableHandle() {}),
                new SchemaTableName("testSchema", "testTable"));
        ConnectorId connectorId = new ConnectorId("connectorId");
        ConnectorTransactionHandle transactionHandle = new ConnectorTransactionHandle() {
        };
        ConnectorTableHandle connectorTableHandle = new ConnectorTableHandle() {
        };
        AnalyzeTableHandle analyzeTableHandle = new AnalyzeTableHandle(connectorId, transactionHandle, connectorTableHandle);
        ConnectorTransactionHandle connectorTranscationHandle = new ConnectorTransactionHandle() {
        };
        ConnectorTableLayoutHandle connectorTableLayoutHandle = new ConnectorTableLayoutHandle() {
        };
        TableWriteInfo.DeleteScanInfo deleteScanInfo = new TableWriteInfo.DeleteScanInfo(new PlanNodeId(""),
                new TableHandle(connectorId, connectorTableHandle, connectorTranscationHandle, Optional.of(connectorTableLayoutHandle)));
        return Optional.of(
                new TableWriteInfo(Optional.of(createHandle), Optional.of(analyzeTableHandle), Optional.of(deleteScanInfo)));
    }

    private OutputBuffers getOutputIds()
    {
        Map<OutputBuffers.OutputBufferId, Integer> buffers = new HashMap<>();
        return new OutputBuffers(OutputBuffers.BufferType.PARTITIONED, 123L, true, buffers);
    }

    private List<TaskSource> getSources()
    {
        TaskSource taskSource = new TaskSource(new PlanNodeId(""), ImmutableSet.of(), true);
        return ImmutableList.of(taskSource);
    }

    private Map<String, String> getExtraCredentials()
    {
        return new HashMap<>();
    }

    private SessionRepresentation getSessionRepresentation()
    {
        return new SessionRepresentation(
                "123",
                Optional.empty(),
                true,
                "user",
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                TimeZoneKey.UTC_KEY,
                Locale.ENGLISH,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableSet.of(),
                new ResourceEstimates(0, 0, 0, 0),
                1L,
                new HashMap<>(),
                new HashMap<>(),
                new HashMap<>(),
                new HashMap<>(),
                new HashMap<>());
    }
}