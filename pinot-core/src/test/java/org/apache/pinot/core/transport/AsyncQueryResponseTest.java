/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.transport;

import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.transport.AsyncQueryResponse.ResponseOutcome;
import org.apache.pinot.core.transport.QueryRequestPlan.RequestGroup;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class AsyncQueryResponseTest {
  private static final long REQUEST_ID = 123L;
  private static final long START_TIME_MS = 1_000_000L;
  private static final long TIMEOUT_MS = 1_000L;
  private static final ServerRoutingInstance PRIMARY_SERVER =
      new ServerRoutingInstance("primary-host", 1234, TableType.OFFLINE);
  private static final ServerRoutingInstance HEDGE_SERVER =
      new ServerRoutingInstance("hedge-host", 1235, TableType.OFFLINE);

  private QueryRouter _queryRouter;
  private ServerRoutingStatsManager _serverRoutingStatsManager;

  @BeforeMethod
  public void setUp() {
    _queryRouter = mock(QueryRouter.class);
    _serverRoutingStatsManager = mock(ServerRoutingStatsManager.class);
  }

  @Test
  public void testPrimaryOnlyBehaviorRemainsCompatible()
      throws Exception {
    AsyncQueryResponse asyncQueryResponse =
        new AsyncQueryResponse(_queryRouter, REQUEST_ID, Set.of(PRIMARY_SERVER), START_TIME_MS, TIMEOUT_MS,
            _serverRoutingStatsManager);
    DataTable cleanResponse = createDataTable();

    asyncQueryResponse.markRequestSubmitted(PRIMARY_SERVER);
    ResponseOutcome outcome = asyncQueryResponse.receiveDataTable(PRIMARY_SERVER, cleanResponse, 17, 3);
    Map<ServerRoutingInstance, ServerResponse> finalResponses = asyncQueryResponse.getFinalResponses();

    assertEquals(outcome, ResponseOutcome.NONE);
    assertEquals(asyncQueryResponse.getStatus(), QueryResponse.Status.COMPLETED);
    assertEquals(asyncQueryResponse.getNumLogicalRequests(), 1);
    assertEquals(asyncQueryResponse.getNumServersResponded(), 1);
    assertNull(asyncQueryResponse.getHedgeStats());
    assertEquals(finalResponses.size(), 1);
    assertTrue(finalResponses.containsKey(PRIMARY_SERVER));
    assertSame(finalResponses.get(PRIMARY_SERVER).getDataTable(), cleanResponse);
    verify(_serverRoutingStatsManager).recordStatsForQuerySubmission(REQUEST_ID, PRIMARY_SERVER.getInstanceId());
    verify(_serverRoutingStatsManager).recordStatsUponResponseArrival(eq(REQUEST_ID),
        eq(PRIMARY_SERVER.getInstanceId()), eq((long) finalResponses.get(PRIMARY_SERVER).getResponseDelayMs()));
    verify(_queryRouter).markQueryDone(REQUEST_ID);
  }

  @Test
  public void testRegisteredHedgeCanWinAndFinalResponsesContainSingleLogicalResponse()
      throws Exception {
    AsyncQueryResponse asyncQueryResponse = createHedgedResponse();
    DataTable hedgeResponse = createDataTable();

    assertTrue(asyncQueryResponse.tryRegisterHedge(PRIMARY_SERVER));
    asyncQueryResponse.markRequestSubmitted(PRIMARY_SERVER);
    asyncQueryResponse.markRequestSubmitted(HEDGE_SERVER);

    ResponseOutcome outcome = asyncQueryResponse.receiveDataTable(HEDGE_SERVER, hedgeResponse, 21, 4);
    Map<ServerRoutingInstance, ServerResponse> finalResponses = asyncQueryResponse.getFinalResponses();

    assertEquals(outcome, ResponseOutcome.HEDGE_WIN);
    assertEquals(asyncQueryResponse.getStatus(), QueryResponse.Status.COMPLETED);
    assertEquals(asyncQueryResponse.getNumLogicalRequests(), 1);
    assertEquals(asyncQueryResponse.getNumServersResponded(), 1);
    assertEquals(finalResponses.size(), 1);
    assertFalse(finalResponses.containsKey(PRIMARY_SERVER));
    assertTrue(finalResponses.containsKey(HEDGE_SERVER));
    assertSame(finalResponses.get(HEDGE_SERVER).getDataTable(), hedgeResponse);
    verify(_serverRoutingStatsManager).recordStatsForQuerySubmission(REQUEST_ID, PRIMARY_SERVER.getInstanceId());
    verify(_serverRoutingStatsManager).recordStatsForQuerySubmission(REQUEST_ID, HEDGE_SERVER.getInstanceId());
  }

  @Test
  public void testPrimaryCanWinAfterHedgeRegistrationAndLateHedgeIsIgnoredForReduction()
      throws Exception {
    AsyncQueryResponse asyncQueryResponse = createHedgedResponse();
    DataTable primaryResponse = createDataTable();

    assertTrue(asyncQueryResponse.tryRegisterHedge(PRIMARY_SERVER));
    asyncQueryResponse.markRequestSubmitted(PRIMARY_SERVER);
    asyncQueryResponse.markRequestSubmitted(HEDGE_SERVER);
    asyncQueryResponse.markHedgeRequestDispatched(HEDGE_SERVER);

    ResponseOutcome primaryOutcome = asyncQueryResponse.receiveDataTable(PRIMARY_SERVER, primaryResponse, 19, 2);
    Map<ServerRoutingInstance, ServerResponse> finalResponses = asyncQueryResponse.getFinalResponses();
    ResponseOutcome hedgeOutcome = asyncQueryResponse.receiveDataTable(HEDGE_SERVER, createDataTable(), 23, 2);

    assertEquals(primaryOutcome, ResponseOutcome.PRIMARY_WIN);
    assertEquals(hedgeOutcome, ResponseOutcome.LOSER);
    assertEquals(finalResponses.size(), 1);
    assertTrue(finalResponses.containsKey(PRIMARY_SERVER));
    assertFalse(finalResponses.containsKey(HEDGE_SERVER));
    assertSame(finalResponses.get(PRIMARY_SERVER).getDataTable(), primaryResponse);
  }

  @Test
  public void testClientErrorIsUsableImmediately()
      throws Exception {
    AsyncQueryResponse asyncQueryResponse = createHedgedResponse();
    DataTable clientErrorResponse = createDataTable(QueryErrorCode.QUERY_CANCELLATION);

    assertTrue(asyncQueryResponse.tryRegisterHedge(PRIMARY_SERVER));
    asyncQueryResponse.markRequestSubmitted(PRIMARY_SERVER);
    asyncQueryResponse.markRequestSubmitted(HEDGE_SERVER);
    asyncQueryResponse.markHedgeRequestDispatched(HEDGE_SERVER);

    ResponseOutcome outcome =
        asyncQueryResponse.receiveDataTable(PRIMARY_SERVER, clientErrorResponse, 11, 1);
    Map<ServerRoutingInstance, ServerResponse> finalResponses = asyncQueryResponse.getFinalResponses();

    assertEquals(outcome, ResponseOutcome.PRIMARY_WIN);
    assertEquals(finalResponses.size(), 1);
    assertTrue(finalResponses.containsKey(PRIMARY_SERVER));
    assertSame(finalResponses.get(PRIMARY_SERVER).getDataTable(), clientErrorResponse);
  }

  @Test
  public void testServerErrorWaitsForRegisteredDuplicateAndCleanDuplicateWins()
      throws Exception {
    AsyncQueryResponse asyncQueryResponse = createHedgedResponse();
    DataTable primaryErrorResponse = createDataTable(QueryErrorCode.SERVER_TABLE_MISSING);
    DataTable hedgeResponse = createDataTable();

    assertTrue(asyncQueryResponse.tryRegisterHedge(PRIMARY_SERVER));
    asyncQueryResponse.markRequestSubmitted(PRIMARY_SERVER);
    asyncQueryResponse.markRequestSubmitted(HEDGE_SERVER);

    ResponseOutcome primaryOutcome =
        asyncQueryResponse.receiveDataTable(PRIMARY_SERVER, primaryErrorResponse, 13, 1);
    ResponseOutcome hedgeOutcome = asyncQueryResponse.receiveDataTable(HEDGE_SERVER, hedgeResponse, 17, 2);
    Map<ServerRoutingInstance, ServerResponse> finalResponses = asyncQueryResponse.getFinalResponses();

    assertEquals(primaryOutcome, ResponseOutcome.NONE);
    assertEquals(asyncQueryResponse.getStatus(), QueryResponse.Status.COMPLETED);
    assertEquals(hedgeOutcome, ResponseOutcome.HEDGE_WIN);
    assertEquals(finalResponses.size(), 1);
    assertTrue(finalResponses.containsKey(HEDGE_SERVER));
    assertSame(finalResponses.get(HEDGE_SERVER).getDataTable(), hedgeResponse);
  }

  @Test
  public void testUnknownErrorCodeIsTreatedAsServerError()
      throws Exception {
    AsyncQueryResponse asyncQueryResponse = createHedgedResponse();
    DataTable unknownErrorResponse = createDataTable();
    unknownErrorResponse.addException(Integer.MAX_VALUE, "unknown error");
    DataTable hedgeResponse = createDataTable();

    assertTrue(asyncQueryResponse.tryRegisterHedge(PRIMARY_SERVER));
    asyncQueryResponse.markRequestSubmitted(PRIMARY_SERVER);
    asyncQueryResponse.markRequestSubmitted(HEDGE_SERVER);

    assertEquals(asyncQueryResponse.receiveDataTable(PRIMARY_SERVER, unknownErrorResponse, 13, 1),
        ResponseOutcome.NONE);
    assertEquals(asyncQueryResponse.receiveDataTable(HEDGE_SERVER, hedgeResponse, 17, 2),
        ResponseOutcome.HEDGE_WIN);
    assertSame(asyncQueryResponse.getFinalResponses().get(HEDGE_SERVER).getDataTable(), hedgeResponse);
  }

  @Test
  public void testBothServerErrorsPreservePrimaryError()
      throws Exception {
    AsyncQueryResponse asyncQueryResponse = createHedgedResponse();
    DataTable primaryErrorResponse = createDataTable(QueryErrorCode.SERVER_TABLE_MISSING);
    DataTable hedgeErrorResponse = createDataTable(QueryErrorCode.SERVER_SEGMENT_MISSING);

    assertTrue(asyncQueryResponse.tryRegisterHedge(PRIMARY_SERVER));
    asyncQueryResponse.markRequestSubmitted(PRIMARY_SERVER);
    asyncQueryResponse.markRequestSubmitted(HEDGE_SERVER);

    ResponseOutcome primaryOutcome =
        asyncQueryResponse.receiveDataTable(PRIMARY_SERVER, primaryErrorResponse, 13, 1);
    ResponseOutcome hedgeOutcome =
        asyncQueryResponse.receiveDataTable(HEDGE_SERVER, hedgeErrorResponse, 17, 2);
    Map<ServerRoutingInstance, ServerResponse> finalResponses = asyncQueryResponse.getFinalResponses();

    assertEquals(primaryOutcome, ResponseOutcome.NONE);
    assertEquals(hedgeOutcome, ResponseOutcome.ALL_ATTEMPTS_FAILED);
    assertEquals(finalResponses.size(), 1);
    assertTrue(finalResponses.containsKey(PRIMARY_SERVER));
    assertSame(finalResponses.get(PRIMARY_SERVER).getDataTable(), primaryErrorResponse);
  }

  @Test
  public void testHedgeServerErrorIsPreservedWhenPrimaryTransportFailsLater()
      throws Exception {
    AsyncQueryResponse asyncQueryResponse = createHedgedResponse();
    DataTable hedgeErrorResponse = createDataTable(QueryErrorCode.SERVER_SEGMENT_MISSING);

    assertTrue(asyncQueryResponse.tryRegisterHedge(PRIMARY_SERVER));
    asyncQueryResponse.markRequestSubmitted(PRIMARY_SERVER);
    asyncQueryResponse.markRequestSubmitted(HEDGE_SERVER);

    assertEquals(asyncQueryResponse.receiveDataTable(HEDGE_SERVER, hedgeErrorResponse, 17, 2),
        ResponseOutcome.NONE);
    assertTrue(asyncQueryResponse.markServerDown(PRIMARY_SERVER, new IllegalStateException("primary channel down")));
    Map<ServerRoutingInstance, ServerResponse> finalResponses = asyncQueryResponse.getFinalResponses();

    assertEquals(finalResponses.size(), 1);
    assertTrue(finalResponses.containsKey(HEDGE_SERVER));
    assertSame(finalResponses.get(HEDGE_SERVER).getDataTable(), hedgeErrorResponse);
  }

  @Test
  public void testPrimaryChannelFailureIsRetainedWhenHedgeRescuesQuery()
      throws Exception {
    AsyncQueryResponse asyncQueryResponse = createHedgedResponse();

    assertTrue(asyncQueryResponse.tryRegisterHedge(PRIMARY_SERVER));
    asyncQueryResponse.markRequestSubmitted(PRIMARY_SERVER);
    asyncQueryResponse.markRequestSubmitted(HEDGE_SERVER);
    asyncQueryResponse.markHedgeRequestDispatched(HEDGE_SERVER);
    assertTrue(asyncQueryResponse.markServerDown(PRIMARY_SERVER, new IllegalStateException("primary channel down")));
    asyncQueryResponse.receiveDataTable(HEDGE_SERVER, createDataTable(), 17, 2);
    Map<ServerRoutingInstance, ServerResponse> finalResponses = asyncQueryResponse.getFinalResponses();

    assertEquals(asyncQueryResponse.getStatus(), QueryResponse.Status.COMPLETED);
    assertTrue(finalResponses.containsKey(HEDGE_SERVER));
  }

  @Test
  public void testBothTransportFailuresFailQueryAndPreservePrimaryFailure()
      throws InterruptedException {
    AsyncQueryResponse asyncQueryResponse = createHedgedResponse();
    IllegalStateException primaryFailure = new IllegalStateException("primary channel down");

    assertTrue(asyncQueryResponse.tryRegisterHedge(PRIMARY_SERVER));
    asyncQueryResponse.markRequestSubmitted(PRIMARY_SERVER);
    asyncQueryResponse.markHedgeRequestDispatched(HEDGE_SERVER);
    asyncQueryResponse.markServerDown(PRIMARY_SERVER, primaryFailure);
    asyncQueryResponse.markServerDown(HEDGE_SERVER, new IllegalStateException("hedge channel down"));
    Map<ServerRoutingInstance, ServerResponse> finalResponses = asyncQueryResponse.getFinalResponses();

    assertEquals(asyncQueryResponse.getStatus(), QueryResponse.Status.FAILED);
    assertEquals(asyncQueryResponse.getFailedServer(), PRIMARY_SERVER);
    assertSame(asyncQueryResponse.getException(), primaryFailure);
    assertEquals(finalResponses.size(), 1);
    assertTrue(finalResponses.containsKey(PRIMARY_SERVER));
    assertNull(finalResponses.get(PRIMARY_SERVER).getDataTable());
    verify(_queryRouter).onAllHedgeAttemptsFailed("testTable");
  }

  @Test
  public void testPrimaryOnlyDuplicateResponsesDoNotEmitHedgeOutcome()
      throws Exception {
    AsyncQueryResponse asyncQueryResponse =
        new AsyncQueryResponse(_queryRouter, REQUEST_ID, Set.of(PRIMARY_SERVER), START_TIME_MS, TIMEOUT_MS,
            _serverRoutingStatsManager);
    DataTable cleanResponse = createDataTable();

    asyncQueryResponse.markRequestSubmitted(PRIMARY_SERVER);
    ResponseOutcome firstOutcome = asyncQueryResponse.receiveDataTable(PRIMARY_SERVER, cleanResponse, 10, 1);
    ResponseOutcome duplicateOutcome = asyncQueryResponse.receiveDataTable(PRIMARY_SERVER, createDataTable(), 12, 2);

    assertEquals(firstOutcome, ResponseOutcome.NONE);
    assertEquals(duplicateOutcome, ResponseOutcome.NONE);
  }

  @Test
  public void testDuplicateHedgedResponsesAreIgnored()
      throws Exception {
    AsyncQueryResponse asyncQueryResponse = createHedgedResponse();
    DataTable cleanResponse = createDataTable();

    assertTrue(asyncQueryResponse.tryRegisterHedge(PRIMARY_SERVER));
    asyncQueryResponse.markRequestSubmitted(PRIMARY_SERVER);
    asyncQueryResponse.markRequestSubmitted(HEDGE_SERVER);
    asyncQueryResponse.markHedgeRequestDispatched(HEDGE_SERVER);
    asyncQueryResponse.receiveDataTable(PRIMARY_SERVER, cleanResponse, 10, 1);
    ResponseOutcome duplicateOutcome = asyncQueryResponse.receiveDataTable(PRIMARY_SERVER, createDataTable(), 12, 2);

    assertEquals(duplicateOutcome, ResponseOutcome.DUPLICATE);
  }

  @Test
  public void testLateLoserBeforeDeadlineRecordsActualLatencyOnce()
      throws Exception {
    AsyncQueryResponse asyncQueryResponse = createHedgedResponse();

    assertTrue(asyncQueryResponse.tryRegisterHedge(PRIMARY_SERVER));
    asyncQueryResponse.markRequestSubmitted(PRIMARY_SERVER);
    asyncQueryResponse.markRequestSubmitted(HEDGE_SERVER);
    asyncQueryResponse.receiveDataTable(HEDGE_SERVER, createDataTable(), 21, 2);
    asyncQueryResponse.getFinalResponses();

    verify(_serverRoutingStatsManager, never()).recordStatsUponResponseArrival(eq(REQUEST_ID),
        eq(PRIMARY_SERVER.getInstanceId()), anyLong());

    ResponseOutcome loserOutcome = asyncQueryResponse.receiveDataTable(PRIMARY_SERVER, createDataTable(), 13, 1);
    long loserLatency =
        asyncQueryResponse.getCurrentResponses().get(PRIMARY_SERVER).getResponseDelayMs();
    asyncQueryResponse.expireAtDeadline();

    assertEquals(loserOutcome, ResponseOutcome.LOSER);
    verify(_serverRoutingStatsManager, times(1)).recordStatsUponResponseArrival(eq(REQUEST_ID),
        eq(PRIMARY_SERVER.getInstanceId()), eq(loserLatency));
  }

  @Test
  public void testMissingLoserAtDeadlineGetsTimeoutAccountingWithoutDoubleDecrement()
      throws Exception {
    AsyncQueryResponse asyncQueryResponse = createHedgedResponse();

    assertTrue(asyncQueryResponse.tryRegisterHedge(PRIMARY_SERVER));
    asyncQueryResponse.markRequestSubmitted(PRIMARY_SERVER);
    asyncQueryResponse.markRequestSubmitted(HEDGE_SERVER);
    asyncQueryResponse.receiveDataTable(HEDGE_SERVER, createDataTable(), 21, 2);
    asyncQueryResponse.getFinalResponses();

    verify(_serverRoutingStatsManager, never()).recordStatsUponResponseArrival(eq(REQUEST_ID),
        eq(PRIMARY_SERVER.getInstanceId()), anyLong());

    asyncQueryResponse.expireAtDeadline();
    ResponseOutcome postDeadlineOutcome =
        asyncQueryResponse.receiveDataTable(PRIMARY_SERVER, createDataTable(), 13, 1);

    assertEquals(postDeadlineOutcome, ResponseOutcome.DUPLICATE);
    verify(_serverRoutingStatsManager, times(1)).recordStatsUponResponseArrival(eq(REQUEST_ID),
        eq(PRIMARY_SERVER.getInstanceId()), eq(TIMEOUT_MS));
  }

  @Test
  public void testHedgeFailureDoesNotContaminatePrimaryFailureState()
      throws Exception {
    AsyncQueryResponse asyncQueryResponse = createHedgedResponse();

    assertTrue(asyncQueryResponse.tryRegisterHedge(PRIMARY_SERVER));
    asyncQueryResponse.markRequestSubmitted(PRIMARY_SERVER);
    asyncQueryResponse.markRequestSubmitted(HEDGE_SERVER);
    asyncQueryResponse.markHedgeSendFailed(HEDGE_SERVER, new IllegalStateException("hedge failed"));
    ResponseOutcome primaryOutcome =
        asyncQueryResponse.receiveDataTable(PRIMARY_SERVER, createDataTable(), 17, 2);
    Map<ServerRoutingInstance, ServerResponse> finalResponses = asyncQueryResponse.getFinalResponses();

    assertEquals(primaryOutcome, ResponseOutcome.NONE);
    assertEquals(asyncQueryResponse.getStatus(), QueryResponse.Status.COMPLETED);
    assertNull(asyncQueryResponse.getFailedServer());
    assertNull(asyncQueryResponse.getException());
    assertSame(finalResponses.get(PRIMARY_SERVER).getDataTable(),
        asyncQueryResponse.getCurrentResponses().get(PRIMARY_SERVER).getDataTable());
  }

  @Test
  public void testHedgeServerDownDoesNotContaminatePrimaryFailureState()
      throws Exception {
    AsyncQueryResponse asyncQueryResponse = createHedgedResponse();

    assertTrue(asyncQueryResponse.tryRegisterHedge(PRIMARY_SERVER));
    asyncQueryResponse.markRequestSubmitted(PRIMARY_SERVER);
    asyncQueryResponse.markRequestSubmitted(HEDGE_SERVER);
    asyncQueryResponse.markHedgeRequestDispatched(HEDGE_SERVER);
    assertTrue(asyncQueryResponse.markServerDown(HEDGE_SERVER, new IllegalStateException("hedge channel down")));
    asyncQueryResponse.receiveDataTable(PRIMARY_SERVER, createDataTable(), 17, 2);
    Map<ServerRoutingInstance, ServerResponse> finalResponses = asyncQueryResponse.getFinalResponses();

    assertEquals(asyncQueryResponse.getStatus(), QueryResponse.Status.COMPLETED);
    assertNull(asyncQueryResponse.getFailedServer());
    assertNull(asyncQueryResponse.getException());
    assertTrue(finalResponses.containsKey(PRIMARY_SERVER));
  }

  @Test
  public void testHedgeChannelDownBeforeDispatchDoesNotTerminalizeAttempt() {
    AsyncQueryResponse asyncQueryResponse = createHedgedResponse();

    assertTrue(asyncQueryResponse.tryRegisterHedge(PRIMARY_SERVER));
    asyncQueryResponse.markRequestSubmitted(PRIMARY_SERVER);
    assertFalse(
        asyncQueryResponse.markServerDown(HEDGE_SERVER, new IllegalStateException("channel down before dispatch")));

    verify(_queryRouter, never()).onHedgeAttemptTerminal(eq("testTable"), eq(
        asyncQueryResponse.getCurrentResponses().get(HEDGE_SERVER)));
    assertTrue(asyncQueryResponse.getHedgeStats().contains("sent=false"));

    asyncQueryResponse.markHedgeSendFailed(HEDGE_SERVER, new IllegalStateException("send failed"));

    verify(_queryRouter).onHedgeAttemptTerminal(eq("testTable"), eq(
        asyncQueryResponse.getCurrentResponses().get(HEDGE_SERVER)));
  }

  @Test
  public void testFailedQueryRetainsOutstandingHedgeUntilDeadline()
      throws InterruptedException {
    AsyncQueryResponse asyncQueryResponse = createHedgedResponse();

    assertTrue(asyncQueryResponse.tryRegisterHedge(PRIMARY_SERVER));
    asyncQueryResponse.markRequestSubmitted(PRIMARY_SERVER);
    asyncQueryResponse.markRequestSubmitted(HEDGE_SERVER);
    asyncQueryResponse.markQueryFailed(PRIMARY_SERVER, new IllegalStateException("another primary failed"));
    asyncQueryResponse.getFinalResponses();

    verify(_queryRouter, never()).markQueryDone(REQUEST_ID);
    verify(_queryRouter, never()).onHedgeAttemptTerminal(eq("testTable"), eq(
        asyncQueryResponse.getCurrentResponses().get(HEDGE_SERVER)));
    verify(_serverRoutingStatsManager, never()).recordStatsUponResponseArrival(eq(REQUEST_ID),
        eq(HEDGE_SERVER.getInstanceId()), anyLong());

    asyncQueryResponse.expireAtDeadline();

    verify(_queryRouter).onHedgeAttemptTerminal(eq("testTable"), eq(
        asyncQueryResponse.getCurrentResponses().get(HEDGE_SERVER)));
    verify(_queryRouter).markQueryDone(REQUEST_ID);
    verify(_serverRoutingStatsManager).recordStatsUponResponseArrival(REQUEST_ID, HEDGE_SERVER.getInstanceId(),
        TIMEOUT_MS);
  }

  @Test
  public void testUnresolvedHedgedGroupAtDeadlineRecordsAllAttemptsFailed() {
    AsyncQueryResponse asyncQueryResponse = createHedgedResponse();

    assertTrue(asyncQueryResponse.tryRegisterHedge(PRIMARY_SERVER));
    asyncQueryResponse.markRequestSubmitted(PRIMARY_SERVER);
    asyncQueryResponse.markRequestSubmitted(HEDGE_SERVER);
    asyncQueryResponse.expireAtDeadline();

    assertEquals(asyncQueryResponse.getStatus(), QueryResponse.Status.TIMED_OUT);
    verify(_queryRouter).onAllHedgeAttemptsFailed("testTable");
  }

  private AsyncQueryResponse createHedgedResponse() {
    RequestGroup requestGroup =
        new RequestGroup(PRIMARY_SERVER, mock(org.apache.pinot.common.request.InstanceRequest.class), HEDGE_SERVER,
            mock(org.apache.pinot.common.request.InstanceRequest.class),
            new ServerInstance(HEDGE_SERVER.getHostname(), HEDGE_SERVER.getPort()));
    QueryRequestPlan queryRequestPlan = new QueryRequestPlan(Map.of(PRIMARY_SERVER, requestGroup));
    return new AsyncQueryResponse(_queryRouter, REQUEST_ID, "testTable", queryRequestPlan, START_TIME_MS, TIMEOUT_MS,
        _serverRoutingStatsManager);
  }

  private static DataTable createDataTable(QueryErrorCode... errorCodes)
      throws Exception {
    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(REQUEST_ID));
    for (QueryErrorCode errorCode : errorCodes) {
      dataTable.addException(errorCode, errorCode.name());
    }
    assertNotNull(dataTable);
    return dataTable;
  }
}
