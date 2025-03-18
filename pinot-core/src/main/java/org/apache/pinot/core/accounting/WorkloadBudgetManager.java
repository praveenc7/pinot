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
package org.apache.pinot.core.accounting;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WorkloadBudgetManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkloadBudgetManager.class);

  private static final AtomicReference<WorkloadBudgetManager> INSTANCE = new AtomicReference<>();

  private final long _enforcementWindowMs;
  private final ConcurrentHashMap<String, Budget> _workloadBudgets = new ConcurrentHashMap<>();
  private final ScheduledExecutorService _resetScheduler = Executors.newSingleThreadScheduledExecutor();

  public static void init(long enforcementWindowMs) {
    if (INSTANCE.compareAndSet(null, new WorkloadBudgetManager(enforcementWindowMs))) {
      LOGGER.info("Initialized WorkloadBudgetManager with enforcementWindowMs: {}", enforcementWindowMs);
    } else {
      LOGGER.error("WorkloadBudgetManager is already initialized, not initializing it again");
    }
  }

  @Nullable
  public static WorkloadBudgetManager getInstance() {
    // NOTE: In some tests, ServerQueryLogger might not be initialized. Returns null when it is not initialized.
    return INSTANCE.get();
  }

  private WorkloadBudgetManager(long enforcementWindowMs) {
    this._enforcementWindowMs = enforcementWindowMs;

    // TODO(Vivek): We need a config to enable this only when necessary.
    startBudgetResetTask();
  }

  /**
   * Adds or updates budget for a workload (Thread-Safe).
   */
  public void addOrUpdateWorkload(String workload, long cpuBudgetNs, long memoryBudgetBytes) {
    _workloadBudgets.compute(workload, (key, existingBudget) -> new Budget(cpuBudgetNs, memoryBudgetBytes));
    LOGGER.info("Updated budget for workload: {} -> CPU: {}ns, Memory: {} bytes", workload, cpuBudgetNs,
        memoryBudgetBytes);
  }

  /**
   * Attempts to charge CPU and memory usage against the workload budget (Thread-Safe).
   */
  public BudgetStats tryCharge(String workload, long cpuUsedNs, long memoryUsedBytes) {
    Budget budget = _workloadBudgets.get(workload);
    if (budget == null) {
      LOGGER.warn("No budget found for workload: {}", workload);
      return new BudgetStats(0, 0);
    }
    return budget.tryCharge(cpuUsedNs, memoryUsedBytes);
  }

  /**
   * Retrieves the remaining budget for a specific workload (Thread-Safe).
   */
  public BudgetStats getRemainingBudgetForWorkload(String workload) {
    Budget budget = _workloadBudgets.get(workload);
    return budget != null ? budget.getStats() : new BudgetStats(0, 0);
  }

  /**
   * Retrieves the total remaining budget across all workloads (Thread-Safe).
   */
  public BudgetStats getRemainingBudgetAcrossAllWorkloads() {
    long totalCpuRemaining =
        _workloadBudgets.values().stream().mapToLong(budget -> budget.getStats().cpuRemaining).sum();
    long totalMemRemaining =
        _workloadBudgets.values().stream().mapToLong(budget -> budget.getStats().memoryRemaining).sum();
    return new BudgetStats(totalCpuRemaining, totalMemRemaining);
  }

  /**
   * Periodically resets budgets at the end of each enforcement window (Thread-Safe).
   */
  private void startBudgetResetTask() {
    LOGGER.info("Starting budget reset task with enforcement window: {}ms", _enforcementWindowMs);
    _resetScheduler.scheduleAtFixedRate(() -> {
      LOGGER.info("Resetting all workload budgets.");
      // Also print the budget used in the last enforcement window.
      _workloadBudgets.forEach((workload, budget) -> {
        BudgetStats stats = budget.getStats();
        LOGGER.info("Workload: {} -> CPU: {}ns, Memory: {} bytes", workload, stats.cpuRemaining, stats.memoryRemaining);
        // Reset the budget.
        budget.reset();
      });
    }, _enforcementWindowMs, _enforcementWindowMs, TimeUnit.MILLISECONDS);
  }

  /**
   * Represents remaining budget stats.
   */
  public static class BudgetStats {
    public final long cpuRemaining;
    public final long memoryRemaining;

    public BudgetStats(long cpuRemaining, long memoryRemaining) {
      this.cpuRemaining = cpuRemaining;
      this.memoryRemaining = memoryRemaining;
    }
  }

  /**
   * Internal class representing a budget with CPU and memory constraints.
   */
  public class Budget {
    private final long _initialCpuBudget;
    private final long _initialMemoryBudget;

    // **Use AtomicLong for safe concurrent updates**
    private final AtomicLong _cpuRemaining;
    private final AtomicLong _memoryRemaining;

    public Budget(long cpuBudgetNs, long memoryBudgetBytes) {
      this._initialCpuBudget = cpuBudgetNs;
      this._initialMemoryBudget = memoryBudgetBytes;
      this._cpuRemaining = new AtomicLong(cpuBudgetNs);
      this._memoryRemaining = new AtomicLong(memoryBudgetBytes);
    }

    /**
     * Attempts to charge CPU and memory usage atomically.
     * Uses a CAS loop to ensure safe concurrent updates.
     *
     * @return BudgetStats representing the remaining CPU and memory after the charge.
     *         If budget is exceeded, it returns the current remaining budget without modifying it.
     */
    public BudgetStats tryCharge(long cpuUsedNs, long memoryUsedBytes) {
      while (true) {
        // Capture current values
        long currentCpu = _cpuRemaining.get();
        long currentMem = _memoryRemaining.get();

        if (cpuUsedNs == 0 && memoryUsedBytes == 0) {
          return new BudgetStats(currentCpu, currentMem);
        }

        // Attempt to deduct both CPU and Memory budgets atomically
        if (_cpuRemaining.compareAndSet(currentCpu, currentCpu - cpuUsedNs) && _memoryRemaining.compareAndSet(
            currentMem, currentMem - memoryUsedBytes)) {
          return new BudgetStats(currentCpu - cpuUsedNs, currentMem - memoryUsedBytes); // Successfully charged
        }

        // If one of the compareAndSet operations failed, retry
      }
    }

    /**
     * Resets the budget back to its original limits.
     */
    public void reset() {
      _cpuRemaining.set(_initialCpuBudget);
      _memoryRemaining.set(_initialMemoryBudget);
    }

    /**
     * Gets the current remaining budget.
     */
    public BudgetStats getStats() {
      return new BudgetStats(_cpuRemaining.get(), _memoryRemaining.get());
    }
  }
}
