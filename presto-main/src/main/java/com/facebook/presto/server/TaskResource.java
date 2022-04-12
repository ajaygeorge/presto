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
package com.facebook.presto.server;

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.airlift.json.Codec;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.smile.SmileCodec;
import com.facebook.airlift.stats.TimeStat;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.presto.Session;
import com.facebook.presto.common.Page;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.buffer.BufferResult;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.metadata.ConnectorMetadataUpdateHandleSerde;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.MetadataUpdates;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.operator.DriverStats;
import com.facebook.presto.operator.ExchangeClientStatus;
import com.facebook.presto.operator.HashCollisionsInfo;
import com.facebook.presto.operator.JoinOperatorInfo;
import com.facebook.presto.operator.OperatorInfo;
import com.facebook.presto.operator.OperatorInfoUnion;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.PipelineStats;
import com.facebook.presto.operator.SplitOperatorInfo;
import com.facebook.presto.operator.TableFinishInfo;
import com.facebook.presto.operator.TableWriterMergeInfo;
import com.facebook.presto.operator.TableWriterOperator;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.operator.WindowInfo;
import com.facebook.presto.operator.exchange.LocalExchangeBufferInfo;
import com.facebook.presto.operator.repartition.PartitionedOutputInfo;
import com.facebook.presto.server.thrift.Any;
import com.facebook.presto.spi.ConnectorMetadataUpdateHandle;
import com.facebook.presto.spi.page.SerializedPage;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.CompletionCallback;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.MoreFutures.addTimeout;
import static com.facebook.airlift.http.client.thrift.ThriftRequestUtils.APPLICATION_THRIFT_BINARY;
import static com.facebook.airlift.http.client.thrift.ThriftRequestUtils.APPLICATION_THRIFT_COMPACT;
import static com.facebook.airlift.http.client.thrift.ThriftRequestUtils.APPLICATION_THRIFT_FB_COMPACT;
import static com.facebook.airlift.http.server.AsyncResponseHandler.bindAsyncResponse;
import static com.facebook.presto.PrestoMediaTypes.APPLICATION_JACKSON_SMILE;
import static com.facebook.presto.PrestoMediaTypes.PRESTO_PAGES;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_BUFFER_COMPLETE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CURRENT_STATE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_SIZE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_WAIT;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_NEXT_TOKEN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_TOKEN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TASK_INSTANCE_ID;
import static com.facebook.presto.server.security.RoleType.INTERNAL;
import static com.facebook.presto.util.TaskUtils.DEFAULT_MAX_WAIT_TIME;
import static com.facebook.presto.util.TaskUtils.randomizeWaitTime;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

/**
 * Manages tasks on this worker node
 */
@Path("/v1/task")
@RolesAllowed(INTERNAL)
public class TaskResource
{
    private static final Duration ADDITIONAL_WAIT_TIME = new Duration(5, SECONDS);

    private final TaskManager taskManager;
    private final SessionPropertyManager sessionPropertyManager;
    private final Executor responseExecutor;
    private final ScheduledExecutorService timeoutExecutor;
    private final TimeStat readFromOutputBufferTime = new TimeStat();
    private final TimeStat resultsRequestTime = new TimeStat();
    private final Codec<PlanFragment> planFragmentCodec;
    private final InternalCommunicationConfig communicationConfig;
    private final HandleResolver handleResolver;
    private final ThriftCodecManager thriftCodecManager;
    private final ConnectorMetadataUpdateHandleSerde<ConnectorMetadataUpdateHandle> connectorMetadataUpdateHandleSerde;

    @Inject
    public TaskResource(
            TaskManager taskManager,
            SessionPropertyManager sessionPropertyManager,
            @ForAsyncRpc BoundedExecutor responseExecutor,
            @ForAsyncRpc ScheduledExecutorService timeoutExecutor,
            JsonCodec<PlanFragment> planFragmentJsonCodec,
            SmileCodec<PlanFragment> planFragmentSmileCodec,
            InternalCommunicationConfig communicationConfig,
            HandleResolver handleResolver,
            ThriftCodecManager thriftCodecManager)
    {
        this.taskManager = requireNonNull(taskManager, "taskManager is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.responseExecutor = requireNonNull(responseExecutor, "responseExecutor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
        this.planFragmentCodec = planFragmentJsonCodec;
        this.communicationConfig = requireNonNull(communicationConfig, "communicationConfig is null");
        this.handleResolver = requireNonNull(handleResolver, "handleResolver is null");
        this.thriftCodecManager = requireNonNull(thriftCodecManager, "thriftCodecManager is null");
        this.connectorMetadataUpdateHandleSerde = new ConnectorMetadataUpdateHandleSerde<>(thriftCodecManager, communicationConfig.getThriftProtocol());
    }

    @GET
    @Consumes({APPLICATION_JSON, APPLICATION_JACKSON_SMILE})
    @Produces({APPLICATION_JSON, APPLICATION_JACKSON_SMILE})
    public List<TaskInfo> getAllTaskInfo(@Context UriInfo uriInfo)
    {
        List<TaskInfo> allTaskInfo = taskManager.getAllTaskInfo();
        if (shouldSummarize(uriInfo)) {
            allTaskInfo = ImmutableList.copyOf(transform(allTaskInfo, TaskInfo::summarize));
        }
        return allTaskInfo;
    }

    @POST
    @Path("{taskId}")
    @Consumes({APPLICATION_JSON, APPLICATION_JACKSON_SMILE})
    @Produces({APPLICATION_JSON, APPLICATION_JACKSON_SMILE})
    public Response createOrUpdateTask(@PathParam("taskId") TaskId taskId, TaskUpdateRequest taskUpdateRequest, @Context UriInfo uriInfo)
    {
        requireNonNull(taskUpdateRequest, "taskUpdateRequest is null");

        Session session = taskUpdateRequest.getSession().toSession(sessionPropertyManager, taskUpdateRequest.getExtraCredentials());
        TaskInfo taskInfo = taskManager.updateTask(session,
                taskId,
                taskUpdateRequest.getFragment().map(planFragmentCodec::fromBytes),
                taskUpdateRequest.getSources(),
                taskUpdateRequest.getOutputIds(),
                taskUpdateRequest.getTableWriteInfo());

        if (shouldSummarize(uriInfo)) {
            taskInfo = taskInfo.summarize();
        }

        return Response.ok().entity(taskInfo).build();
    }

    @GET
    @Path("{taskId}")
    @Consumes({APPLICATION_JSON, APPLICATION_JACKSON_SMILE, APPLICATION_THRIFT_BINARY, APPLICATION_THRIFT_COMPACT, APPLICATION_THRIFT_FB_COMPACT})
    @Produces({APPLICATION_JSON, APPLICATION_JACKSON_SMILE, APPLICATION_THRIFT_BINARY, APPLICATION_THRIFT_COMPACT, APPLICATION_THRIFT_FB_COMPACT})
    public void getTaskInfo(
            @PathParam("taskId") final TaskId taskId,
            @HeaderParam(PRESTO_CURRENT_STATE) TaskState currentState,
            @HeaderParam(PRESTO_MAX_WAIT) Duration maxWait,
            @Context UriInfo uriInfo,
            @Context HttpHeaders httpHeaders,
            @Suspended AsyncResponse asyncResponse)
    {
        requireNonNull(taskId, "taskId is null");

        if (currentState == null || maxWait == null) {
            TaskInfo taskInfo = taskManager.getTaskInfo(taskId);
            if (shouldSummarize(uriInfo)) {
                taskInfo = taskInfo.summarize();
            }

            boolean isThriftRequest = httpHeaders.getAcceptableMediaTypes().stream()
                    .anyMatch(mediaType -> mediaType.toString().contains("thrift"));

            if (isThriftRequest) {
                taskInfo = thriftify(taskInfo);
            }
            asyncResponse.resume(taskInfo);
            return;
        }

        Duration waitTime = randomizeWaitTime(maxWait);
        ListenableFuture<TaskInfo> futureTaskInfo = addTimeout(
                taskManager.getTaskInfo(taskId, currentState),
                () -> thriftify(taskManager.getTaskInfo(taskId)),
                waitTime,
                timeoutExecutor);

        if (shouldSummarize(uriInfo)) {
            futureTaskInfo = Futures.transform(futureTaskInfo, TaskInfo::summarize, directExecutor());
        }

        // For hard timeout, add an additional time to max wait for thread scheduling contention and GC
        Duration timeout = new Duration(waitTime.toMillis() + ADDITIONAL_WAIT_TIME.toMillis(), MILLISECONDS);
        bindAsyncResponse(asyncResponse, futureTaskInfo, responseExecutor)
                .withTimeout(timeout);
    }

    private TaskInfo thriftify(TaskInfo taskInfo)
    {
        return new TaskInfo(
                taskInfo.getTaskId(),
                taskInfo.getTaskStatus(),
                taskInfo.getLastHeartbeat(),
                taskInfo.getOutputBuffers(),
                taskInfo.getNoMoreSplits(),
                thriftifyTaskStats(taskInfo.getStats()),
                taskInfo.isNeedsPlan(),
                thriftifyMetadataUpdates(taskInfo.getMetadataUpdates()),
                taskInfo.getNodeId());
    }

    private TaskStats thriftifyTaskStats(TaskStats taskStats)
    {
        if (taskStats.getPipelines().isEmpty()) {
            return taskStats;
        }

        return new TaskStats(
                taskStats.getCreateTime(),
                taskStats.getFirstStartTime(),
                taskStats.getLastStartTime(),
                taskStats.getLastEndTime(),
                taskStats.getEndTime(),
                taskStats.getElapsedTimeInNanos(),
                taskStats.getQueuedTimeInNanos(),
                taskStats.getTotalDrivers(),
                taskStats.getQueuedDrivers(),
                taskStats.getQueuedPartitionedDrivers(),
                taskStats.getQueuedPartitionedSplitsWeight(),
                taskStats.getRunningDrivers(),
                taskStats.getRunningPartitionedDrivers(),
                taskStats.getRunningPartitionedSplitsWeight(),
                taskStats.getBlockedDrivers(),
                taskStats.getCompletedDrivers(),
                taskStats.getCumulativeUserMemory(),
                taskStats.getCumulativeTotalMemory(),
                taskStats.getUserMemoryReservationInBytes(),
                taskStats.getRevocableMemoryReservationInBytes(),
                taskStats.getSystemMemoryReservationInBytes(),
                taskStats.getPeakUserMemoryInBytes(),
                taskStats.getPeakTotalMemoryInBytes(),
                taskStats.getPeakNodeTotalMemoryInBytes(),
                taskStats.getTotalScheduledTimeInNanos(),
                taskStats.getTotalCpuTimeInNanos(),
                taskStats.getTotalBlockedTimeInNanos(),
                taskStats.isFullyBlocked(),
                taskStats.getBlockedReasons(),
                taskStats.getTotalAllocationInBytes(),
                taskStats.getRawInputDataSizeInBytes(),
                taskStats.getRawInputPositions(),
                taskStats.getProcessedInputDataSizeInBytes(),
                taskStats.getProcessedInputPositions(),
                taskStats.getOutputDataSizeInBytes(),
                taskStats.getOutputPositions(),
                taskStats.getPhysicalWrittenDataSizeInBytes(),
                taskStats.getFullGcCount(),
                taskStats.getFullGcTimeInMillis(),
                thriftifyPipeLineStatsList(taskStats.getPipelines()),
                taskStats.getRuntimeStats());
    }

    private List<PipelineStats> thriftifyPipeLineStatsList(List<PipelineStats> pipelines)
    {
        return pipelines.stream()
                .map(p -> thriftifyPipelineStats(p))
                .collect(toList());
    }

    private PipelineStats thriftifyPipelineStats(PipelineStats pipelineStats)
    {
        if (pipelineStats.getDrivers().isEmpty() && pipelineStats.getOperatorSummaries().isEmpty()) {
            return pipelineStats;
        }

        return new PipelineStats(
            pipelineStats.getPipelineId(),
            pipelineStats.getFirstStartTime(),
            pipelineStats.getLastStartTime(),
            pipelineStats.getLastEndTime(),
            pipelineStats.isInputPipeline(),
            pipelineStats.isOutputPipeline(),
            pipelineStats.getTotalDrivers(),
            pipelineStats.getQueuedDrivers(),
            pipelineStats.getQueuedPartitionedDrivers(),
            pipelineStats.getQueuedPartitionedSplitsWeight(),
            pipelineStats.getRunningDrivers(),
            pipelineStats.getRunningPartitionedDrivers(),
            pipelineStats.getRunningPartitionedSplitsWeight(),
            pipelineStats.getBlockedDrivers(),
            pipelineStats.getCompletedDrivers(),
            pipelineStats.getUserMemoryReservationInBytes(),
            pipelineStats.getRevocableMemoryReservationInBytes(),
            pipelineStats.getSystemMemoryReservationInBytes(),
            pipelineStats.getQueuedTime(),
            pipelineStats.getElapsedTime(),
            pipelineStats.getTotalScheduledTimeInNanos(),
            pipelineStats.getTotalCpuTimeInNanos(),
            pipelineStats.getTotalBlockedTimeInNanos(),
            pipelineStats.isFullyBlocked(),
            pipelineStats.getBlockedReasons(),
            pipelineStats.getTotalAllocationInBytes(),
            pipelineStats.getRawInputDataSizeInBytes(),
            pipelineStats.getRawInputPositions(),
            pipelineStats.getProcessedInputDataSizeInBytes(),
            pipelineStats.getProcessedInputPositions(),
            pipelineStats.getOutputDataSizeInBytes(),
            pipelineStats.getOutputPositions(),
            pipelineStats.getPhysicalWrittenDataSizeInBytes(),
            convertToThriftOperatorStatsList(pipelineStats.getOperatorSummaries()),
            convertToThriftDriverStatsList(pipelineStats.getDrivers()));
    }

    private List<DriverStats> convertToThriftDriverStatsList(List<DriverStats> drivers)
    {
        return drivers.stream()
                .map(d -> d.getOperatorStats().isEmpty() ? d : convertToThriftDriverStats(d))
                .collect(toList());
    }

    private DriverStats convertToThriftDriverStats(DriverStats driverStats)
    {
        return new DriverStats(
            driverStats.getLifespan(),
            driverStats.getCreateTime(),
            driverStats.getStartTime(),
            driverStats.getEndTime(),
            driverStats.getQueuedTime(),
            driverStats.getElapsedTime(),
            driverStats.getUserMemoryReservation(),
            driverStats.getRevocableMemoryReservation(),
            driverStats.getSystemMemoryReservation(),
            driverStats.getTotalScheduledTime(),
            driverStats.getTotalCpuTime(),
            driverStats.getTotalBlockedTime(),
            driverStats.isFullyBlocked(),
            driverStats.getBlockedReasons(),
            driverStats.getTotalAllocation(),
            driverStats.getRawInputDataSize(),
            driverStats.getRawInputPositions(),
            driverStats.getRawInputReadTime(),
            driverStats.getProcessedInputDataSize(),
            driverStats.getProcessedInputPositions(),
            driverStats.getOutputDataSize(),
            driverStats.getOutputPositions(),
            driverStats.getPhysicalWrittenDataSize(),
            convertToThriftOperatorStatsList(driverStats.getOperatorStats()));
    }

    private List<OperatorStats> convertToThriftOperatorStatsList(List<OperatorStats> operatorSummaries)
    {
        return operatorSummaries.stream()
                .map(os -> os.getInfo() != null ? convertToThriftOperatorStats(os) : os)
                .collect(toList());
    }

    private OperatorStats convertToThriftOperatorStats(OperatorStats operatorStats)
    {
        return new OperatorStats(
            operatorStats.getStageId(),
            operatorStats.getStageExecutionId(),
            operatorStats.getPipelineId(),
            operatorStats.getOperatorId(),
            operatorStats.getPlanNodeId(),
            operatorStats.getOperatorType(),
            operatorStats.getTotalDrivers(),
            operatorStats.getAddInputCalls(),
            operatorStats.getAddInputWall(),
            operatorStats.getAddInputCpu(),
            operatorStats.getAddInputAllocation(),
            operatorStats.getRawInputDataSize(),
            operatorStats.getRawInputPositions(),
            operatorStats.getInputDataSize(),
            operatorStats.getInputPositions(),
            operatorStats.getSumSquaredInputPositions(),
            operatorStats.getGetOutputCalls(),
            operatorStats.getGetOutputWall(),
            operatorStats.getGetOutputCpu(),
            operatorStats.getGetOutputAllocation(),
            operatorStats.getOutputDataSize(),
            operatorStats.getOutputPositions(),
            operatorStats.getPhysicalWrittenDataSize(),
            operatorStats.getAdditionalCpu(),
            operatorStats.getBlockedWall(),
            operatorStats.getFinishCalls(),
            operatorStats.getFinishWall(),
            operatorStats.getFinishCpu(),
            operatorStats.getFinishAllocation(),
            operatorStats.getUserMemoryReservation(),
            operatorStats.getRevocableMemoryReservation(),
            operatorStats.getSystemMemoryReservation(),
            operatorStats.getPeakUserMemoryReservation(),
            operatorStats.getPeakSystemMemoryReservation(),
            operatorStats.getPeakTotalMemoryReservation(),
            operatorStats.getSpilledDataSize(),
            operatorStats.getBlockedReason(),
            operatorStats.getRuntimeStats(),
            convertToThriftOperatorInfo(operatorStats.getInfo()));
    }

    private OperatorInfoUnion convertToThriftOperatorInfo(OperatorInfo info)
    {
        if (info instanceof ExchangeClientStatus) {
            return new OperatorInfoUnion((ExchangeClientStatus) info);
        }
        else if (info instanceof LocalExchangeBufferInfo) {
            return new OperatorInfoUnion((LocalExchangeBufferInfo) info);
        }
        else if (info instanceof TableFinishInfo) {
            return new OperatorInfoUnion((TableFinishInfo) info);
        }
        else if (info instanceof SplitOperatorInfo) {
            return new OperatorInfoUnion((SplitOperatorInfo) info);
        }
        else if (info instanceof HashCollisionsInfo) {
            return new OperatorInfoUnion((HashCollisionsInfo) info);
        }
        else if (info instanceof PartitionedOutputInfo) {
            return new OperatorInfoUnion((PartitionedOutputInfo) info);
        }
        else if (info instanceof JoinOperatorInfo) {
            return new OperatorInfoUnion((JoinOperatorInfo) info);
        }
        else if (info instanceof WindowInfo) {
            return new OperatorInfoUnion((WindowInfo) info);
        }
        else if (info instanceof TableWriterOperator.TableWriterInfo) {
            return new OperatorInfoUnion((TableWriterOperator.TableWriterInfo) info);
        }
        else if (info instanceof TableWriterMergeInfo) {
            return new OperatorInfoUnion((TableWriterMergeInfo) info);
        }
        else {
            return null;
        }
    }

    private MetadataUpdates thriftifyMetadataUpdates(MetadataUpdates metadataUpdates)
    {
        List<ConnectorMetadataUpdateHandle> metadataUpdateHandles = metadataUpdates.getMetadataUpdates();
        List<Any> anyMetadataHandles = convertToAny(metadataUpdateHandles);
        return new MetadataUpdates(metadataUpdates.getConnectorId(), anyMetadataHandles, true);
    }

    private List<Any> convertToAny(List<ConnectorMetadataUpdateHandle> connectorMetadataUpdateHandles)
    {
        List<Any> anyList = connectorMetadataUpdateHandles.stream()
                .map(e -> new Any(handleResolver.getId(e), connectorMetadataUpdateHandleSerde.serialize(e)))
                .collect(toList());
        return anyList;
    }

    @GET
    @Path("{taskId}/status")
    @Consumes({APPLICATION_JSON, APPLICATION_JACKSON_SMILE, APPLICATION_THRIFT_BINARY, APPLICATION_THRIFT_COMPACT, APPLICATION_THRIFT_FB_COMPACT})
    @Produces({APPLICATION_JSON, APPLICATION_JACKSON_SMILE, APPLICATION_THRIFT_BINARY, APPLICATION_THRIFT_COMPACT, APPLICATION_THRIFT_FB_COMPACT})
    public void getTaskStatus(
            @PathParam("taskId") TaskId taskId,
            @HeaderParam(PRESTO_CURRENT_STATE) TaskState currentState,
            @HeaderParam(PRESTO_MAX_WAIT) Duration maxWait,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        requireNonNull(taskId, "taskId is null");

        if (currentState == null || maxWait == null) {
            TaskStatus taskStatus = taskManager.getTaskStatus(taskId);
            asyncResponse.resume(taskStatus);
            return;
        }

        Duration waitTime = randomizeWaitTime(maxWait);
        // TODO: With current implementation, a newly completed driver group won't trigger immediate HTTP response,
        // leading to a slight delay of approx 1 second, which is not a major issue for any query that are heavy weight enough
        // to justify group-by-group execution. In order to fix this, REST endpoint /v1/{task}/status will need change.
        ListenableFuture<TaskStatus> futureTaskStatus = addTimeout(
                taskManager.getTaskStatus(taskId, currentState),
                () -> taskManager.getTaskStatus(taskId),
                waitTime,
                timeoutExecutor);

        // For hard timeout, add an additional time to max wait for thread scheduling contention and GC
        Duration timeout = new Duration(waitTime.toMillis() + ADDITIONAL_WAIT_TIME.toMillis(), MILLISECONDS);
        bindAsyncResponse(asyncResponse, futureTaskStatus, responseExecutor)
                .withTimeout(timeout);
    }

    @POST
    @Path("{taskId}/metadataresults")
    @Consumes({APPLICATION_JSON, APPLICATION_JACKSON_SMILE})
    public Response updateMetadataResults(@PathParam("taskId") TaskId taskId, MetadataUpdates metadataUpdates, @Context UriInfo uriInfo)
    {
        requireNonNull(metadataUpdates, "metadataUpdates is null");
        taskManager.updateMetadataResults(taskId, metadataUpdates);
        return Response.ok().build();
    }

    @DELETE
    @Path("{taskId}")
    @Consumes({APPLICATION_JSON, APPLICATION_JACKSON_SMILE, APPLICATION_THRIFT_BINARY, APPLICATION_THRIFT_COMPACT, APPLICATION_THRIFT_FB_COMPACT})
    @Produces({APPLICATION_JSON, APPLICATION_JACKSON_SMILE, APPLICATION_THRIFT_BINARY, APPLICATION_THRIFT_COMPACT, APPLICATION_THRIFT_FB_COMPACT})
    public TaskInfo deleteTask(
            @PathParam("taskId") TaskId taskId,
            @QueryParam("abort") @DefaultValue("true") boolean abort,
            @Context UriInfo uriInfo)
    {
        requireNonNull(taskId, "taskId is null");
        TaskInfo taskInfo;

        if (abort) {
            taskInfo = taskManager.abortTask(taskId);
        }
        else {
            taskInfo = taskManager.cancelTask(taskId);
        }

        if (shouldSummarize(uriInfo)) {
            taskInfo = taskInfo.summarize();
        }
        return thriftify(taskInfo);
    }

    @GET
    @Path("{taskId}/results/{bufferId}/{token}")
    @Produces(PRESTO_PAGES)
    public void getResults(
            @PathParam("taskId") TaskId taskId,
            @PathParam("bufferId") OutputBufferId bufferId,
            @PathParam("token") final long token,
            @HeaderParam(PRESTO_MAX_SIZE) DataSize maxSize,
            @Suspended AsyncResponse asyncResponse)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");

        long start = System.nanoTime();
        ListenableFuture<BufferResult> bufferResultFuture = taskManager.getTaskResults(taskId, bufferId, token, maxSize);
        Duration waitTime = randomizeWaitTime(DEFAULT_MAX_WAIT_TIME);
        bufferResultFuture = addTimeout(
                bufferResultFuture,
                () -> BufferResult.emptyResults(taskManager.getTaskInstanceId(taskId), token, false),
                waitTime,
                timeoutExecutor);

        ListenableFuture<Response> responseFuture = Futures.transform(bufferResultFuture, result -> {
            List<SerializedPage> serializedPages = result.getSerializedPages();

            GenericEntity<?> entity = null;
            Status status;
            if (serializedPages.isEmpty()) {
                status = Status.NO_CONTENT;
            }
            else {
                entity = new GenericEntity<>(serializedPages, new TypeToken<List<Page>>() {}.getType());
                status = Status.OK;
            }

            return Response.status(status)
                    .entity(entity)
                    .header(PRESTO_TASK_INSTANCE_ID, result.getTaskInstanceId())
                    .header(PRESTO_PAGE_TOKEN, result.getToken())
                    .header(PRESTO_PAGE_NEXT_TOKEN, result.getNextToken())
                    .header(PRESTO_BUFFER_COMPLETE, result.isBufferComplete())
                    .build();
        }, directExecutor());

        // For hard timeout, add an additional time to max wait for thread scheduling contention and GC
        Duration timeout = new Duration(waitTime.toMillis() + ADDITIONAL_WAIT_TIME.toMillis(), MILLISECONDS);
        bindAsyncResponse(asyncResponse, responseFuture, responseExecutor)
                .withTimeout(timeout,
                        Response.status(Status.NO_CONTENT)
                                .header(PRESTO_TASK_INSTANCE_ID, taskManager.getTaskInstanceId(taskId))
                                .header(PRESTO_PAGE_TOKEN, token)
                                .header(PRESTO_PAGE_NEXT_TOKEN, token)
                                .header(PRESTO_BUFFER_COMPLETE, false)
                                .build());

        responseFuture.addListener(() -> readFromOutputBufferTime.add(Duration.nanosSince(start)), directExecutor());
        asyncResponse.register((CompletionCallback) throwable -> resultsRequestTime.add(Duration.nanosSince(start)));
    }

    @GET
    @Path("{taskId}/results/{bufferId}/{token}/acknowledge")
    public void acknowledgeResults(
            @PathParam("taskId") TaskId taskId,
            @PathParam("bufferId") OutputBufferId bufferId,
            @PathParam("token") final long token)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");

        taskManager.acknowledgeTaskResults(taskId, bufferId, token);
    }

    @DELETE
    @Path("{taskId}/results/{bufferId}")
    @Produces(APPLICATION_JSON)
    public void abortResults(@PathParam("taskId") TaskId taskId, @PathParam("bufferId") OutputBufferId bufferId, @Context UriInfo uriInfo)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");

        taskManager.abortTaskResults(taskId, bufferId);
    }

    @DELETE
    @Path("{taskId}/remote-source/{remoteSourceTaskId}")
    public void removeRemoteSource(@PathParam("taskId") TaskId taskId, @PathParam("remoteSourceTaskId") TaskId remoteSourceTaskId)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(remoteSourceTaskId, "remoteSourceTaskId is null");

        taskManager.removeRemoteSource(taskId, remoteSourceTaskId);
    }

    @Managed
    @Nested
    public TimeStat getReadFromOutputBufferTime()
    {
        return readFromOutputBufferTime;
    }

    @Managed
    @Nested
    public TimeStat getResultsRequestTime()
    {
        return resultsRequestTime;
    }

    private static boolean shouldSummarize(UriInfo uriInfo)
    {
        return uriInfo.getQueryParameters().containsKey("summarize");
    }
}
