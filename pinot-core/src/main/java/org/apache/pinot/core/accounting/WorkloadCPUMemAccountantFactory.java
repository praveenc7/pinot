package org.apache.pinot.core.accounting;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;
import org.apache.pinot.spi.accounting.ThreadAccountantFactory;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.accounting.ThreadResourceTracker;
import org.apache.pinot.spi.accounting.ThreadResourceUsageAccountant;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WorkloadCPUMemAccountantFactory implements ThreadAccountantFactory {
  @Override
  public ThreadResourceUsageAccountant init(PinotConfiguration config, String instanceId) {
    return new WorkloadCPUMemAccountantFactory.WorkloadCPUMemResourceUsageAccountant(config, instanceId);
  }

  public static class WorkloadCPUMemResourceUsageAccountant extends Tracing.DefaultThreadResourceUsageAccountant {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(WorkloadCPUMemAccountantFactory.WorkloadCPUMemResourceUsageAccountant.class);
    /**
     * Executor service for the thread accounting task, slightly higher priority than normal priority
     */
    private static final String ACCOUNTANT_TASK_NAME = "WorkloadCPUMemThreadAccountant";
    private static final int ACCOUNTANT_PRIORITY = 4;
    private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(1, r -> {
      Thread thread = new Thread(r);
      thread.setPriority(ACCOUNTANT_PRIORITY);
      thread.setDaemon(true);
      thread.setName(ACCOUNTANT_TASK_NAME);
      return thread;
    });

    // the map to track stats entry for each thread, the entry will automatically be added when one calls
    // setThreadResourceUsageProvider on the thread, including but not limited to
    // server worker thread, runner thread, broker jetty thread, or broker netty thread
    private final ConcurrentHashMap<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> _threadEntriesMap =
        new ConcurrentHashMap<>();

    private final ThreadLocal<CPUMemThreadLevelAccountingObjects.ThreadEntry> _threadLocalEntry =
        ThreadLocal.withInitial(() -> {
          CPUMemThreadLevelAccountingObjects.ThreadEntry ret = new CPUMemThreadLevelAccountingObjects.ThreadEntry();
          _threadEntriesMap.put(Thread.currentThread(), ret);
          LOGGER.info("Adding thread to _threadLocalEntry: {}", Thread.currentThread().getName());
          return ret;
        });

    private final ThreadLocal<ThreadResourceUsageProvider> _threadResourceUsageProvider;

    // track thread cpu time
    private final boolean _isThreadCPUSamplingEnabled;

    // track memory usage
    private final boolean _isThreadMemorySamplingEnabled;

    // the periodical task that aggregates and preempts queries
    private final WorkloadCPUMemAccountantFactory.WorkloadCPUMemResourceUsageAccountant.WatcherTask _watcherTask;

    private final WorkloadBudgetManager _workloadBudgetManager;

    public WorkloadCPUMemResourceUsageAccountant(PinotConfiguration config, String instanceId) {

      LOGGER.info("Initializing WorkloadCPUMemResourceUsageAccountant");

      boolean threadCpuTimeMeasurementEnabled = ThreadResourceUsageProvider.isThreadCpuTimeMeasurementEnabled();
      boolean threadMemoryMeasurementEnabled = ThreadResourceUsageProvider.isThreadMemoryMeasurementEnabled();
      LOGGER.info("threadCpuTimeMeasurementEnabled: {}, threadMemoryMeasurementEnabled: {}",
          threadCpuTimeMeasurementEnabled, threadMemoryMeasurementEnabled);

      boolean cpuSamplingConfig = config.getProperty(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING,
          CommonConstants.Accounting.DEFAULT_ENABLE_THREAD_CPU_SAMPLING);
      boolean memorySamplingConfig =
          config.getProperty(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING,
              CommonConstants.Accounting.DEFAULT_ENABLE_THREAD_MEMORY_SAMPLING);
      LOGGER.info("cpuSamplingConfig: {}, memorySamplingConfig: {}", cpuSamplingConfig, memorySamplingConfig);

      _isThreadCPUSamplingEnabled = cpuSamplingConfig && threadCpuTimeMeasurementEnabled;
      _isThreadMemorySamplingEnabled = memorySamplingConfig && threadMemoryMeasurementEnabled;
      LOGGER.info("_isThreadCPUSamplingEnabled: {}, _isThreadMemorySamplingEnabled: {}", _isThreadCPUSamplingEnabled,
          _isThreadMemorySamplingEnabled);

      // ThreadMXBean wrapper
      _threadResourceUsageProvider = new ThreadLocal<>();
      _watcherTask = new WatcherTask();

      // TODO(Vivek): Provide enforcement window from config
      _workloadBudgetManager = WorkloadBudgetManager.getInstance();
    }

    @Override
    public Collection<? extends ThreadResourceTracker> getThreadResources() {
      return _threadEntriesMap.values();
    }

    @Override
    public void sampleUsage() {
      sampleThreadBytesAllocated();
      sampleThreadCPUTime();
    }

    public void sampleThreadCPUTime() {
      ThreadResourceUsageProvider provider = getThreadResourceUsageProvider();
      if (_isThreadCPUSamplingEnabled && provider != null) {
        _threadLocalEntry.get()._currentThreadCPUTimeSampleMS = provider.getThreadTimeNs();
      }
    }

    public void sampleThreadBytesAllocated() {
      ThreadResourceUsageProvider provider = getThreadResourceUsageProvider();
      if (_isThreadMemorySamplingEnabled && provider != null) {
        _threadLocalEntry.get()._currentThreadMemoryAllocationSampleBytes = provider.getThreadAllocatedBytes();
      }
    }

    private ThreadResourceUsageProvider getThreadResourceUsageProvider() {
      return _threadResourceUsageProvider.get();
    }

    @Override
    public void setThreadResourceUsageProvider(ThreadResourceUsageProvider threadResourceUsageProvider) {
      _threadResourceUsageProvider.set(threadResourceUsageProvider);
    }

    @Override
    public void createExecutionContextInner(@Nullable String queryId, int taskId,
        ThreadExecutionContext.TaskType taskType, @Nullable ThreadExecutionContext parentContext, String workloadName) {
      _threadLocalEntry.get()._errorStatus.set(null);
      if (parentContext == null) {
        // is anchor thread
        assert queryId != null;
        _threadLocalEntry.get()
            .setThreadTaskStatus(queryId, CommonConstants.Accounting.ANCHOR_TASK_ID, taskType, Thread.currentThread(),
                workloadName);
      } else {
        // not anchor thread
        _threadLocalEntry.get()
            .setThreadTaskStatus(queryId, taskId, parentContext.getTaskType(), parentContext.getAnchorThread(),
                workloadName);
      }
    }

    @Override
    public ThreadExecutionContext getThreadExecutionContext() {
      return _threadLocalEntry.get().getCurrentThreadTaskStatus();
    }

    @Override
    public void clear() {
      CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry = _threadLocalEntry.get();
      CPUMemThreadLevelAccountingObjects.TaskEntry currentTaskStatus = threadEntry.getCurrentThreadTaskStatus();

      // clear task info + stats
      threadEntry.setToIdle();
      // clear threadResourceUsageProvider
      _threadResourceUsageProvider.remove();
      // clear _anchorThread
      super.clear();
    }

    @Override
    public void startWatcherTask() {
      EXECUTOR_SERVICE.submit(_watcherTask);
    }

    public void cleanInactive() {
      // TODO(Vivek): Evaluate if needed.
    }

    public Exception getErrorStatus() {
      return _threadLocalEntry.get()._errorStatus.getAndSet(null);
    }

    class WatcherTask implements Runnable {
      @Override
      public void run() {
        LOGGER.info("WorkloadAccountant is running...");
        while (true) {
          try {
            aggregateWorkloadLevelStats();
          } catch (Exception e) {
            LOGGER.error("Error in WatcherTask", e);
          } finally {
            try {
              // TODO(Vivek): Better name for cleanInactive.
              cleanInactive();

              // TODO(Vivek): Make this configurable.
              Thread.sleep(2);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
        }
      }

      public void aggregateWorkloadLevelStats() {
        if (_isThreadCPUSamplingEnabled && _isThreadMemorySamplingEnabled) {
          LOGGER.info("Feature not enabled. Exiting...");
        }

        Map<String, List<CPUMemThreadLevelAccountingObjects.TaskEntry>> allWorkloads = new HashMap<>();
        HashMap<String, Long> finishedTaskCPUCost = new HashMap<>();
        HashMap<String, Long> finishedTaskMemCost = new HashMap<>();

        List<CPUMemThreadLevelAccountingObjects.ThreadEntry> threadEntries =
            new ArrayList<>(_threadEntriesMap.values());

        for (Map.Entry<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> entry : _threadEntriesMap.entrySet()) {
          Thread thread = entry.getKey();
          CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry = entry.getValue();
          threadEntries.add(threadEntry);

          CPUMemThreadLevelAccountingObjects.TaskEntry currentQueryTask = threadEntry.getCurrentThreadTaskStatus();
          if (currentQueryTask == null) {
            // This means that task has finished. Nothing to aggregate.
            // TODO(Vivek): Check if this model of doing a busy aggregation is enough.
            continue;
          }

          String currentTaskWorkload = currentQueryTask.getWorkloadName();
          long currentCPUCost = threadEntry._currentThreadCPUTimeSampleMS;
          long currentMemoryCost = threadEntry._currentThreadMemoryAllocationSampleBytes;

          CPUMemThreadLevelAccountingObjects.TaskEntry prevQueryTask = threadEntry._previousThreadTaskStatus;

          // The cost to charge against the workloadBudgetManager.
          long deltaCPUCost = currentCPUCost;
          long deltaMemCost = currentMemoryCost;

          if (prevQueryTask != null) {
            if (currentQueryTask == prevQueryTask) {
              deltaCPUCost = currentCPUCost - threadEntry._previousThreadCPUTimeSampleMS;
              deltaMemCost = currentMemoryCost - threadEntry._previousThreadMemoryAllocationSampleBytes;
            }
          }

          threadEntry._previousThreadTaskStatus = currentQueryTask;
          threadEntry._previousThreadCPUTimeSampleMS = currentCPUCost;
          threadEntry._previousThreadMemoryAllocationSampleBytes = currentMemoryCost;

          finishedTaskCPUCost.merge(currentTaskWorkload, deltaCPUCost, Long::sum);
          finishedTaskMemCost.merge(currentTaskWorkload, deltaMemCost, Long::sum);

          if (!allWorkloads.containsKey(currentTaskWorkload)) {
            allWorkloads.put(currentTaskWorkload, new ArrayList<>());
          }
          allWorkloads.get(currentTaskWorkload).add(currentQueryTask);
        }

        for (Map.Entry<String, List<CPUMemThreadLevelAccountingObjects.TaskEntry>> workloadEntry : allWorkloads.entrySet()) {
          String workloadName = workloadEntry.getKey();
          List<CPUMemThreadLevelAccountingObjects.TaskEntry> taskEntries = workloadEntry.getValue();
          long finishedCPUCost = finishedTaskCPUCost.getOrDefault(workloadName, 0L);
          long finishedMemCost = finishedTaskMemCost.getOrDefault(workloadName, 0L);

          WorkloadBudgetManager.BudgetStats budgetStats =
              _workloadBudgetManager.tryCharge(workloadName, finishedCPUCost, finishedMemCost);

          if (budgetStats.cpuRemaining <= 0 || budgetStats.memoryRemaining <= 0) {
            // Interrupt all queries in the list
            for (CPUMemThreadLevelAccountingObjects.TaskEntry taskEntry : taskEntries) {
              String queryId = taskEntry.getQueryId();
              Thread anchorThread = taskEntry.getAnchorThread();
              if (!anchorThread.isInterrupted()) {
                LOGGER.info("Killing query: {} and anchorThread:{}", queryId, anchorThread.getName());
                anchorThread.interrupt();
              }
            }
          }
        }
      }
    }
  }
}
