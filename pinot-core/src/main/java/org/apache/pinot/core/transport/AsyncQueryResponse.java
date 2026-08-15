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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.transport.QueryRequestPlan.RequestGroup;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.exception.QueryErrorCode;


/**
 * Asynchronous response state for logical server requests with optional physical hedge attempts.
 */
@ThreadSafe
public class AsyncQueryResponse implements QueryResponse {
  enum ResponseOutcome {
    NONE,
    PRIMARY_WIN,
    HEDGE_WIN,
    ALL_ATTEMPTS_FAILED,
    LOSER,
    DUPLICATE,
    UNKNOWN
  }

  private enum AttemptRole {
    PRIMARY,
    HEDGE
  }

  private final QueryRouter _queryRouter;
  private final long _requestId;
  private final String _rawTableName;
  private final AtomicReference<Status> _status = new AtomicReference<>(Status.IN_PROGRESS);
  private final AtomicInteger _numServersResponded = new AtomicInteger();
  private final Map<ServerRoutingInstance, LogicalRequestGroup> _groupsByPrimary;
  private final ConcurrentHashMap<ServerRoutingInstance, PhysicalAttempt> _attempts = new ConcurrentHashMap<>();
  private final CountDownLatch _countDownLatch;
  private final long _startTimeMs;
  private final long _maxEndTimeMs;
  private final long _timeoutMs;
  private final ServerRoutingStatsManager _serverRoutingStatsManager;
  private final AtomicBoolean _resultReturned = new AtomicBoolean();
  private final AtomicBoolean _hedgeDecisionScheduled = new AtomicBoolean();
  private final AtomicBoolean _hedgingCancelled = new AtomicBoolean();
  private final AtomicBoolean _hedgeRegistered = new AtomicBoolean();
  private final AtomicBoolean _cleanupComplete = new AtomicBoolean();
  private final AtomicBoolean _attemptsExpired = new AtomicBoolean();

  private volatile ServerRoutingInstance _failedServer;
  private volatile Exception _exception;
  private volatile ScheduledFuture<?> _hedgeDecisionFuture;
  private volatile ScheduledFuture<?> _deadlineCleanupFuture;
  private volatile ServerRoutingInstance _hedgedPrimary;
  private volatile ServerRoutingInstance _hedgeServer;

  public AsyncQueryResponse(QueryRouter queryRouter, long requestId, Set<ServerRoutingInstance> serversQueried,
      long startTimeMs, long timeoutMs, ServerRoutingStatsManager serverRoutingStatsManager) {
    this(queryRouter, requestId, "", createPrimaryOnlyGroups(serversQueried), startTimeMs, timeoutMs,
        serverRoutingStatsManager);
  }

  public AsyncQueryResponse(QueryRouter queryRouter, long requestId, String rawTableName,
      QueryRequestPlan queryRequestPlan, long startTimeMs, long timeoutMs,
      ServerRoutingStatsManager serverRoutingStatsManager) {
    this(queryRouter, requestId, rawTableName, queryRequestPlan.getRequestGroups(), startTimeMs, timeoutMs,
        serverRoutingStatsManager);
  }

  private AsyncQueryResponse(QueryRouter queryRouter, long requestId, String rawTableName,
      Map<ServerRoutingInstance, RequestGroup> requestGroups, long startTimeMs, long timeoutMs,
      ServerRoutingStatsManager serverRoutingStatsManager) {
    _queryRouter = queryRouter;
    _requestId = requestId;
    _rawTableName = rawTableName;
    _startTimeMs = startTimeMs;
    _timeoutMs = timeoutMs;
    _maxEndTimeMs = startTimeMs + timeoutMs;
    _serverRoutingStatsManager = serverRoutingStatsManager;
    _groupsByPrimary = new HashMap<>(requestGroups.size());
    for (Map.Entry<ServerRoutingInstance, RequestGroup> entry : requestGroups.entrySet()) {
      entry.getValue().validate();
      PhysicalAttempt primaryAttempt =
          new PhysicalAttempt(entry.getKey(), AttemptRole.PRIMARY, new ServerResponse(startTimeMs));
      LogicalRequestGroup logicalRequestGroup = new LogicalRequestGroup(entry.getValue(), primaryAttempt);
      _groupsByPrimary.put(entry.getKey(), logicalRequestGroup);
      _attempts.put(entry.getKey(), primaryAttempt);
      _serverRoutingStatsManager.recordStatsForQuerySubmission(requestId, entry.getKey().getInstanceId());
    }
    _countDownLatch = new CountDownLatch(requestGroups.size());
  }

  private static Map<ServerRoutingInstance, RequestGroup> createPrimaryOnlyGroups(
      Set<ServerRoutingInstance> serversQueried) {
    Map<ServerRoutingInstance, RequestGroup> requestGroups = new HashMap<>(serversQueried.size());
    for (ServerRoutingInstance server : serversQueried) {
      requestGroups.put(server, new RequestGroup(server, new InstanceRequest(), null, null, null));
    }
    return requestGroups;
  }

  @Override
  public Status getStatus() {
    return _status.get();
  }

  @Override
  public int getNumServersResponded() {
    return _numServersResponded.get();
  }

  public int getNumLogicalRequests() {
    return _groupsByPrimary.size();
  }

  @Override
  public Map<ServerRoutingInstance, ServerResponse> getCurrentResponses() {
    Map<ServerRoutingInstance, ServerResponse> responses = new HashMap<>(_attempts.size());
    for (Map.Entry<ServerRoutingInstance, PhysicalAttempt> entry : _attempts.entrySet()) {
      responses.put(entry.getKey(), entry.getValue()._response);
    }
    return responses;
  }

  @Override
  public Map<ServerRoutingInstance, ServerResponse> getFinalResponses()
      throws InterruptedException {
    try {
      long remainingMs = Math.max(0L, _maxEndTimeMs - System.currentTimeMillis());
      boolean finish = _countDownLatch.await(remainingMs, TimeUnit.MILLISECONDS);
      if (finish) {
        _status.compareAndSet(Status.IN_PROGRESS, Status.COMPLETED);
      } else {
        expireAtDeadline();
      }
      if (_status.get() == Status.FAILED && !_hedgeRegistered.get()) {
        expireAtDeadline();
      }
    } catch (InterruptedException e) {
      _status.compareAndSet(Status.IN_PROGRESS, Status.FAILED);
      _resultReturned.set(true);
      if (_hedgeRegistered.get()) {
        finalizeAvailableStats();
        tryCleanup();
      } else {
        expireAtDeadline();
      }
      throw e;
    }

    _resultReturned.set(true);
    ScheduledFuture<?> hedgeDecisionFuture = _hedgeDecisionFuture;
    if (hedgeDecisionFuture != null) {
      hedgeDecisionFuture.cancel(false);
    }
    finalizeAvailableStats();
    Map<ServerRoutingInstance, ServerResponse> finalResponses = buildFinalResponses();
    tryCleanup();
    return finalResponses;
  }

  private Map<ServerRoutingInstance, ServerResponse> buildFinalResponses() {
    Map<ServerRoutingInstance, ServerResponse> finalResponses = new HashMap<>(_groupsByPrimary.size());
    for (LogicalRequestGroup group : _groupsByPrimary.values()) {
      synchronized (group) {
        ServerRoutingInstance responseServer =
            group._finalServer != null ? group._finalServer : group._requestGroup.getPrimaryServer();
        ServerResponse response = group._finalResponse != null ? group._finalResponse : group._primary._response;
        finalResponses.put(responseServer, response);
      }
    }
    return finalResponses;
  }

  @Override
  public String getServerStats() {
    StringBuilder stringBuilder = new StringBuilder(
        "(Server=SubmitDelayMs,ResponseDelayMs,ResponseSize,DeserializationTimeMs,RequestSentDelayMs)");
    for (LogicalRequestGroup group : _groupsByPrimary.values()) {
      stringBuilder.append(';').append(group._requestGroup.getPrimaryServer().getShortName()).append('=')
          .append(group._primary._response);
    }
    return stringBuilder.toString();
  }

  @Nullable
  public String getHedgeStats() {
    if (!_hedgeDecisionScheduled.get() && _hedgeServer == null) {
      return null;
    }
    String winner = "none";
    String loserState = "none";
    LogicalRequestGroup hedgedGroup = _hedgedPrimary != null ? _groupsByPrimary.get(_hedgedPrimary) : null;
    if (hedgedGroup != null) {
      synchronized (hedgedGroup) {
        if (hedgedGroup._hasUsableWinner && hedgedGroup._finalServer != null) {
          winner = hedgedGroup._finalServer.equals(_hedgeServer) ? "hedge" : "primary";
        }
        PhysicalAttempt loser = winner.equals("hedge") ? hedgedGroup._primary : hedgedGroup._hedge;
        if (loser != null) {
          loserState = loser._terminal ? "complete" : "pending";
        }
      }
    }
    PhysicalAttempt hedgeAttempt = _hedgeServer != null ? _attempts.get(_hedgeServer) : null;
    String hedgeTiming = hedgeAttempt != null ? hedgeAttempt._response.toString() : "notSent";
    boolean hedgeSent = hedgeAttempt != null
        && (hedgeAttempt._requestDispatched || hedgeAttempt._response.getDataTable() != null);
    return String.format("(scheduled=%s,sent=%s,primary=%s,alternate=%s,winner=%s,loser=%s,alternateStats=%s)",
        _hedgeDecisionScheduled.get(), hedgeSent,
        _hedgedPrimary != null ? _hedgedPrimary.getShortName() : "none",
        _hedgeServer != null ? _hedgeServer.getShortName() : "none", winner, loserState, hedgeTiming);
  }

  @Override
  public long getServerResponseDelayMs(ServerRoutingInstance serverRoutingInstance) {
    PhysicalAttempt attempt = _attempts.get(serverRoutingInstance);
    return attempt != null ? attempt._response.getResponseDelayMs() : -1;
  }

  @Nullable
  @Override
  public ServerRoutingInstance getFailedServer() {
    return _failedServer;
  }

  @Nullable
  @Override
  public Exception getException() {
    return _exception;
  }

  @Override
  public long getRequestId() {
    return _requestId;
  }

  @Override
  public long getTimeoutMs() {
    return _timeoutMs;
  }

  String getRawTableName() {
    return _rawTableName;
  }

  long getMaxEndTimeMs() {
    return _maxEndTimeMs;
  }

  List<RequestGroup> getOutstandingHedgeCandidates() {
    if (_status.get() != Status.IN_PROGRESS || _hedgingCancelled.get()) {
      return Collections.emptyList();
    }
    List<RequestGroup> candidates = new ArrayList<>();
    for (LogicalRequestGroup group : _groupsByPrimary.values()) {
      synchronized (group) {
        if (!group._completed && !group._primary._terminal && group._hedge == null
            && group._requestGroup.getAlternateServer() != null) {
          candidates.add(group._requestGroup);
        }
      }
    }
    return candidates;
  }

  boolean tryRegisterHedge(ServerRoutingInstance primaryServer) {
    if (_hedgingCancelled.get() || !_hedgeRegistered.compareAndSet(false, true)) {
      return false;
    }
    LogicalRequestGroup group = _groupsByPrimary.get(primaryServer);
    if (group == null) {
      _hedgeRegistered.set(false);
      return false;
    }
    synchronized (group) {
      if (_hedgingCancelled.get() || _status.get() != Status.IN_PROGRESS || group._completed
          || group._primary._terminal || group._hedge != null) {
        _hedgeRegistered.set(false);
        return false;
      }
      ServerRoutingInstance alternateServer = group._requestGroup.getAlternateServer();
      if (alternateServer == null) {
        _hedgeRegistered.set(false);
        return false;
      }
      PhysicalAttempt hedgeAttempt =
          new PhysicalAttempt(alternateServer, AttemptRole.HEDGE, new ServerResponse(_startTimeMs));
      group._hedge = hedgeAttempt;
      _hedgedPrimary = primaryServer;
      _hedgeServer = alternateServer;
      if (_attempts.putIfAbsent(alternateServer, hedgeAttempt) != null) {
        group._hedge = null;
        _hedgedPrimary = null;
        _hedgeServer = null;
        _hedgeRegistered.set(false);
        return false;
      }
      try {
        _serverRoutingStatsManager.recordStatsForQuerySubmission(_requestId, alternateServer.getInstanceId());
        return true;
      } catch (RuntimeException e) {
        _attempts.remove(alternateServer, hedgeAttempt);
        group._hedge = null;
        _hedgedPrimary = null;
        _hedgeServer = null;
        _hedgeRegistered.set(false);
        throw e;
      }
    }
  }

  @Nullable
  RequestGroup getRequestGroup(ServerRoutingInstance primaryServer) {
    LogicalRequestGroup group = _groupsByPrimary.get(primaryServer);
    return group != null ? group._requestGroup : null;
  }

  void markHedgeDecisionScheduled(ScheduledFuture<?> hedgeDecisionFuture) {
    _hedgeDecisionScheduled.set(true);
    _hedgeDecisionFuture = hedgeDecisionFuture;
    if (_hedgingCancelled.get()) {
      hedgeDecisionFuture.cancel(false);
    }
  }

  void cancelHedging() {
    _hedgingCancelled.set(true);
    ScheduledFuture<?> hedgeDecisionFuture = _hedgeDecisionFuture;
    if (hedgeDecisionFuture != null) {
      hedgeDecisionFuture.cancel(false);
    }
  }

  boolean isHedgingCancelled() {
    return _hedgingCancelled.get();
  }

  void markDeadlineCleanupScheduled(ScheduledFuture<?> deadlineCleanupFuture) {
    _deadlineCleanupFuture = deadlineCleanupFuture;
    tryCleanup();
  }

  void markRequestSubmitted(ServerRoutingInstance serverRoutingInstance) {
    PhysicalAttempt attempt = _attempts.get(serverRoutingInstance);
    if (attempt != null) {
      attempt._response.markRequestSubmitted();
    }
  }

  void markRequestSent(ServerRoutingInstance serverRoutingInstance, int requestSentLatencyMs) {
    PhysicalAttempt attempt = _attempts.get(serverRoutingInstance);
    if (attempt != null) {
      attempt._response.markRequestSent(requestSentLatencyMs);
    }
  }

  void markHedgeRequestDispatched(ServerRoutingInstance hedgeServer) {
    PhysicalAttempt attempt = _attempts.get(hedgeServer);
    if (attempt == null || attempt._role != AttemptRole.HEDGE) {
      return;
    }
    LogicalRequestGroup group = getGroupForAttempt(attempt);
    synchronized (group) {
      attempt._requestDispatched = true;
    }
  }

  ResponseOutcome receiveDataTable(ServerRoutingInstance serverRoutingInstance, DataTable dataTable, int responseSize,
      int deserializationTimeMs) {
    PhysicalAttempt attempt = _attempts.get(serverRoutingInstance);
    if (attempt == null) {
      return ResponseOutcome.UNKNOWN;
    }
    LogicalRequestGroup group = getGroupForAttempt(attempt);
    ResponseOutcome outcome;
    synchronized (group) {
      if (attempt._terminal) {
        return group._hedge != null ? ResponseOutcome.DUPLICATE : ResponseOutcome.NONE;
      }
      attempt._response.receiveDataTable(dataTable, responseSize, deserializationTimeMs);
      attempt._serverError = hasServerError(dataTable);
      attempt._terminal = true;
      if (group._completed) {
        outcome = ResponseOutcome.LOSER;
      } else if (!attempt._serverError) {
        group._hasUsableWinner = true;
        completeGroup(group, serverRoutingInstance, attempt._response);
        if (attempt._role == AttemptRole.HEDGE) {
          outcome = ResponseOutcome.HEDGE_WIN;
        } else if (group._hedge != null && group._hedge._requestDispatched) {
          outcome = ResponseOutcome.PRIMARY_WIN;
        } else {
          outcome = ResponseOutcome.NONE;
        }
      } else {
        PhysicalAttempt otherAttempt = attempt._role == AttemptRole.PRIMARY ? group._hedge : group._primary;
        if (otherAttempt != null && !otherAttempt._terminal) {
          outcome = ResponseOutcome.NONE;
        } else {
          completeGroupWithRetainedError(group);
          outcome = group._hedge != null ? ResponseOutcome.ALL_ATTEMPTS_FAILED : ResponseOutcome.NONE;
        }
      }
    }
    onAttemptTerminal(attempt);
    return outcome;
  }

  void markHedgeSendFailed(ServerRoutingInstance hedgeServer, Exception exception) {
    PhysicalAttempt attempt = _attempts.get(hedgeServer);
    if (attempt == null) {
      return;
    }
    LogicalRequestGroup group = getGroupForAttempt(attempt);
    boolean allAttemptsFailed = false;
    boolean failQuery = false;
    synchronized (group) {
      if (attempt._terminal) {
        return;
      }
      attempt._terminal = true;
      attempt._exception = exception;
      if (!group._completed && group._primary._terminal) {
        if (hasRetainedError(group)) {
          completeGroupWithRetainedError(group);
        } else {
          failQuery = true;
        }
        allAttemptsFailed = true;
      }
    }
    onAttemptTerminal(attempt);
    if (failQuery) {
      Exception primaryException =
          group._primary._exception != null ? group._primary._exception : exception;
      markQueryFailed(group._primary._server, primaryException);
    }
    if (allAttemptsFailed) {
      _queryRouter.onAllHedgeAttemptsFailed(_rawTableName);
    }
  }

  void markQueryFailed(ServerRoutingInstance serverRoutingInstance, Exception exception) {
    _status.set(Status.FAILED);
    _failedServer = serverRoutingInstance;
    _exception = exception;
    for (LogicalRequestGroup group : _groupsByPrimary.values()) {
      synchronized (group) {
        if (!group._completed) {
          completeGroup(group, group._requestGroup.getPrimaryServer(), group._primary._response);
        }
      }
    }
  }

  /**
   * The server might not be hit by the query. Only fail the query if the query was sent and has not responded.
   */
  boolean markServerDown(ServerRoutingInstance serverRoutingInstance, Exception exception) {
    PhysicalAttempt attempt = _attempts.get(serverRoutingInstance);
    if (attempt == null) {
      return false;
    }
    LogicalRequestGroup group = getGroupForAttempt(attempt);
    boolean failQuery = false;
    boolean allAttemptsFailed = false;
    synchronized (group) {
      if (attempt._terminal) {
        return false;
      }
      if (attempt._role == AttemptRole.HEDGE && !attempt._requestDispatched) {
        return false;
      }
      attempt._terminal = true;
      attempt._exception = exception;
      if (!group._completed) {
        PhysicalAttempt otherAttempt = attempt._role == AttemptRole.PRIMARY ? group._hedge : group._primary;
        if (otherAttempt != null && !otherAttempt._terminal) {
          // The duplicate can still produce a usable response.
        } else if (attempt._role == AttemptRole.PRIMARY && group._hedge == null) {
          failQuery = true;
        } else {
          if (hasRetainedError(group)) {
            completeGroupWithRetainedError(group);
          } else {
            failQuery = true;
          }
          allAttemptsFailed = true;
        }
      }
    }
    onAttemptTerminal(attempt);
    if (failQuery) {
      Exception primaryException =
          group._primary._exception != null ? group._primary._exception : exception;
      markQueryFailed(group._primary._server, primaryException);
    }
    if (allAttemptsFailed) {
      _queryRouter.onAllHedgeAttemptsFailed(_rawTableName);
    }
    return true;
  }

  void skipServerResponse(ServerRoutingInstance serverRoutingInstance) {
    PhysicalAttempt attempt = _attempts.get(serverRoutingInstance);
    if (attempt == null) {
      return;
    }
    LogicalRequestGroup group = getGroupForAttempt(attempt);
    synchronized (group) {
      if (attempt._terminal) {
        return;
      }
      attempt._terminal = true;
      if (!group._completed) {
        PhysicalAttempt otherAttempt = attempt._role == AttemptRole.PRIMARY ? group._hedge : group._primary;
        if (otherAttempt == null || otherAttempt._terminal) {
          completeGroupWithRetainedError(group);
        }
      }
    }
    onAttemptTerminal(attempt);
  }

  void expireAtDeadline() {
    _attemptsExpired.set(true);
    if (_countDownLatch.getCount() == 0) {
      _status.compareAndSet(Status.IN_PROGRESS, Status.COMPLETED);
    } else {
      _status.compareAndSet(Status.IN_PROGRESS, Status.TIMED_OUT);
    }
    boolean allHedgeAttemptsFailed = false;
    for (LogicalRequestGroup group : _groupsByPrimary.values()) {
      synchronized (group) {
        if (!group._completed) {
          allHedgeAttemptsFailed |= group._hedge != null;
          completeGroupWithRetainedError(group);
        }
        expireAttempt(group._primary);
        if (group._hedge != null) {
          expireAttempt(group._hedge);
        }
      }
    }
    if (allHedgeAttemptsFailed) {
      _queryRouter.onAllHedgeAttemptsFailed(_rawTableName);
    }
    finalizeAvailableStats();
    tryCleanup();
  }

  private void expireAttempt(PhysicalAttempt attempt) {
    if (!attempt._terminal) {
      attempt._terminal = true;
      attempt._deadlineExpired = true;
      onAttemptTerminal(attempt);
    }
  }

  private LogicalRequestGroup getGroupForAttempt(PhysicalAttempt attempt) {
    if (attempt._role == AttemptRole.PRIMARY) {
      return _groupsByPrimary.get(attempt._server);
    }
    return _groupsByPrimary.get(_hedgedPrimary);
  }

  private void completeGroupWithRetainedError(LogicalRequestGroup group) {
    PhysicalAttempt errorAttempt =
        group._primary._response.getDataTable() != null ? group._primary : group._hedge;
    if (errorAttempt != null && errorAttempt._response.getDataTable() != null) {
      completeGroup(group, errorAttempt._server, errorAttempt._response);
    } else {
      completeGroup(group, group._requestGroup.getPrimaryServer(), group._primary._response);
    }
  }

  private static boolean hasRetainedError(LogicalRequestGroup group) {
    return group._primary._response.getDataTable() != null
        || (group._hedge != null && group._hedge._response.getDataTable() != null);
  }

  private void completeGroup(LogicalRequestGroup group, ServerRoutingInstance finalServer,
      ServerResponse finalResponse) {
    if (group._completed) {
      return;
    }
    group._completed = true;
    group._finalServer = finalServer;
    group._finalResponse = finalResponse;
    if (finalResponse.getDataTable() != null) {
      _numServersResponded.incrementAndGet();
    }
    _countDownLatch.countDown();
  }

  private void onAttemptTerminal(PhysicalAttempt attempt) {
    if (attempt._role == AttemptRole.HEDGE && attempt._hedgePermitReleased.compareAndSet(false, true)) {
      _queryRouter.onHedgeAttemptTerminal(_rawTableName, attempt._response);
    }
    if (_resultReturned.get()) {
      recordAttemptStats(attempt);
      tryCleanup();
    }
  }

  private void finalizeAvailableStats() {
    boolean forcePending =
        _status.get() == Status.TIMED_OUT || (_status.get() == Status.FAILED && !_hedgeRegistered.get());
    for (PhysicalAttempt attempt : _attempts.values()) {
      if (attempt._terminal || forcePending) {
        recordAttemptStats(attempt);
      }
    }
  }

  private void recordAttemptStats(PhysicalAttempt attempt) {
    if (!attempt._statsRecorded.compareAndSet(false, true)) {
      return;
    }
    long latency;
    if (attempt._response.getDataTable() == null || attempt._serverError || attempt._deadlineExpired
        || attempt._exception != null) {
      latency = _timeoutMs;
    } else {
      latency = attempt._response.getResponseDelayMs();
    }
    _serverRoutingStatsManager.recordStatsUponResponseArrival(_requestId, attempt._server.getInstanceId(), latency);
  }

  private void tryCleanup() {
    if (!_resultReturned.get() && !_attemptsExpired.get()) {
      return;
    }
    for (PhysicalAttempt attempt : _attempts.values()) {
      if (!attempt._statsRecorded.get()) {
        return;
      }
    }
    if (!_cleanupComplete.compareAndSet(false, true)) {
      return;
    }
    ScheduledFuture<?> deadlineCleanupFuture = _deadlineCleanupFuture;
    if (deadlineCleanupFuture != null) {
      deadlineCleanupFuture.cancel(false);
    }
    _queryRouter.markQueryDone(_requestId);
  }

  private static boolean hasServerError(DataTable dataTable) {
    for (Integer errorCode : dataTable.getExceptions().keySet()) {
      QueryErrorCode queryErrorCode;
      try {
        queryErrorCode = QueryErrorCode.fromErrorCode(errorCode);
      } catch (IllegalArgumentException e) {
        return true;
      }
      if (!queryErrorCode.isClientError()) {
        return true;
      }
    }
    return false;
  }

  private static class LogicalRequestGroup {
    private final RequestGroup _requestGroup;
    private final PhysicalAttempt _primary;
    private PhysicalAttempt _hedge;
    private boolean _completed;
    private boolean _hasUsableWinner;
    private ServerRoutingInstance _finalServer;
    private ServerResponse _finalResponse;

    private LogicalRequestGroup(RequestGroup requestGroup, PhysicalAttempt primary) {
      _requestGroup = requestGroup;
      _primary = primary;
    }
  }

  private static class PhysicalAttempt {
    private final ServerRoutingInstance _server;
    private final AttemptRole _role;
    private final ServerResponse _response;
    private final AtomicBoolean _statsRecorded = new AtomicBoolean();
    private final AtomicBoolean _hedgePermitReleased = new AtomicBoolean();
    private volatile boolean _requestDispatched;
    private boolean _terminal;
    private boolean _serverError;
    private boolean _deadlineExpired;
    private Exception _exception;

    private PhysicalAttempt(ServerRoutingInstance server, AttemptRole role, ServerResponse response) {
      _server = server;
      _role = role;
      _response = response;
    }
  }
}
