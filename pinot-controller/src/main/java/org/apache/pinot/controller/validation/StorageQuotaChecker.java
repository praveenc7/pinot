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
package org.apache.pinot.controller.validation;

import com.google.common.base.Preconditions;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.DataSizeUtils;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to check if a new segment is within the configured storage quota for the table
 *
 */
public class StorageQuotaChecker {
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageQuotaChecker.class);

  // Retry configuration for reading the table size from servers. Each server request is retried independently (see
  // MultiHttpRequest), so only servers that fail transiently (e.g. a server is briefly unreachable) are retried while
  // servers that already responded successfully are not re-read.
  private static final int TABLE_SIZE_READ_MAX_ATTEMPTS = 3;
  private static final long TABLE_SIZE_READ_RETRY_INITIAL_DELAY_MS = 1000L;

  private final TableSizeReader _tableSizeReader;
  private final ControllerMetrics _controllerMetrics;
  private final LeadControllerManager _leadControllerManager;
  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final boolean _isEnabled;
  private final int _timeoutMs;
  private final RetryPolicy _tableSizeReadRetryPolicy;

  public StorageQuotaChecker(TableSizeReader tableSizeReader,
      ControllerMetrics controllerMetrics, LeadControllerManager leadControllerManager,
      PinotHelixResourceManager pinotHelixResourceManager, ControllerConf controllerConf) {
    this(tableSizeReader, controllerMetrics, leadControllerManager, pinotHelixResourceManager, controllerConf,
        RetryPolicies.fixedDelayRetryPolicy(TABLE_SIZE_READ_MAX_ATTEMPTS, TABLE_SIZE_READ_RETRY_INITIAL_DELAY_MS));
  }

  public StorageQuotaChecker(TableSizeReader tableSizeReader,
      ControllerMetrics controllerMetrics, LeadControllerManager leadControllerManager,
      PinotHelixResourceManager pinotHelixResourceManager, ControllerConf controllerConf,
      RetryPolicy tableSizeReadRetryPolicy) {
    _tableSizeReader = tableSizeReader;
    _controllerMetrics = controllerMetrics;
    _leadControllerManager = leadControllerManager;
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _isEnabled = controllerConf.getEnableStorageQuotaCheck();
    _timeoutMs = controllerConf.getServerAdminRequestTimeoutSeconds() * 1000;
    Preconditions.checkArgument(_timeoutMs > 0, "Timeout value must be > 0, input: %s", _timeoutMs);
    _tableSizeReadRetryPolicy = tableSizeReadRetryPolicy;
  }

  public static class QuotaCheckerResponse {
    public boolean _isSegmentWithinQuota;
    public String _reason;

    QuotaCheckerResponse(boolean isSegmentWithinQuota, String reason) {
      _isSegmentWithinQuota = isSegmentWithinQuota;
      _reason = reason;
    }
  }

  public static QuotaCheckerResponse success(String msg) {
    return new QuotaCheckerResponse(true, msg);
  }

  public static QuotaCheckerResponse failure(String msg) {
    return new QuotaCheckerResponse(false, msg);
  }

  /**
   * Returns whether the new added segment is within the storage quota.
   */
  public QuotaCheckerResponse isSegmentStorageWithinQuota(TableConfig tableConfig, String segmentName,
      long tarSegmentSizeInBytes, long untarredSegmentSizeInBytes)
      throws InvalidConfigException {
    if (!_isEnabled) {
      return success("Storage quota check is disabled, skipping the check");
    }

    // 1. Read table config
    // 2. read table size from all the servers
    // 3. update predicted segment sizes
    // 4. is the updated size within quota
    QuotaConfig quotaConfig = tableConfig.getQuotaConfig();
    int numReplicas = _pinotHelixResourceManager.getNumReplicas(tableConfig);

    final String tableNameWithType = tableConfig.getTableName();

    if (quotaConfig == null || quotaConfig.getStorage() == null) {
      // no quota configuration...so ignore for backwards compatibility
      String message =
          String.format("Storage quota is not configured for table: %s, skipping the check", tableNameWithType);
      LOGGER.info(message);
      return success(message);
    }

    long allowedStorageBytesPerReplica = quotaConfig.getStorageInBytes();
    long allowedStorageBytes = numReplicas * allowedStorageBytesPerReplica;
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.TABLE_QUOTA, allowedStorageBytes);
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.TABLE_QUOTA_PER_REPLICA,
        allowedStorageBytesPerReplica);

    // read table size
    TableSizeReader.TableSubTypeSizeDetails tableSubtypeSize;
    try {
      tableSubtypeSize =
          _tableSizeReader.getTableSubtypeSize(tableNameWithType, _timeoutMs, true, _tableSizeReadRetryPolicy);
    } catch (InvalidConfigException e) {
      LOGGER.error("Failed to get table size for table {}", tableNameWithType, e);
      throw e;
    }

    if (tableSubtypeSize._estimatedSizeInBytes == -1) {
      // don't fail the quota check in this case
      return success("Missing size reports from all servers. Bypassing storage quota check for " + tableNameWithType);
    }

    // The logic inside this if block is applicable for missing segments as well as
    // when we are checking the quota for only existing segments (segmentSizeInBytes == 0)
    // as in both cases quota is checked across existing segments estimated size alone
    if (untarredSegmentSizeInBytes == 0 || tableSubtypeSize._missingSegments > 0) {
      emitStorageQuotaUtilizationMetric(tableNameWithType, tableSubtypeSize, allowedStorageBytesPerReplica);
      if (tableSubtypeSize._reportedSizePerReplicaInBytes > allowedStorageBytesPerReplica) {
        return failure("Table " + tableNameWithType + " per replica already over quota. Estimated size for per "
            + "replica and all replicas is " + DataSizeUtils.fromBytes(tableSubtypeSize._reportedSizePerReplicaInBytes)
            + " and " + DataSizeUtils.fromBytes(tableSubtypeSize._estimatedSizeInBytes) + ". Configured size for "
            + numReplicas + " is " + DataSizeUtils.fromBytes(allowedStorageBytes));
      } else {
        if (tableSubtypeSize._missingSegments > 0) {
          return success("Missing size report for " + tableSubtypeSize._missingSegments
              + " segments. Bypassing storage quota check for " + tableNameWithType);
        }
        return success("Table " + tableNameWithType + " within quota. Reported per-replica size "
            + DataSizeUtils.fromBytes(tableSubtypeSize._reportedSizePerReplicaInBytes)
            + " is within configured per-replica quota " + DataSizeUtils.fromBytes(allowedStorageBytesPerReplica));
      }
    }

    // If the segment exists(refresh), get the existing size
    TableSizeReader.SegmentSizeDetails sizeDetails = tableSubtypeSize._segments.get(segmentName);
    long existingSegmentSizeBytes = sizeDetails != null ? sizeDetails._maxReportedSizePerReplicaInBytes : 0;
    SegmentZKMetadata existingSegmentZkMetadata =
        _pinotHelixResourceManager.getSegmentZKMetadata(tableNameWithType, segmentName);
    long existingTarSegmentSize = existingSegmentZkMetadata != null
        ? _pinotHelixResourceManager.getSegmentZKMetadata(tableNameWithType, segmentName).getSizeInBytes() : 0;

    // Since tableNameWithType comes with the table type(OFFLINE), thus we guarantee that
    // tableSubtypeSize.estimatedSizeInBytes is the offline table size.
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.OFFLINE_TABLE_ESTIMATED_SIZE,
        tableSubtypeSize._estimatedSizeInBytes);

    LOGGER.info("Table {}'s estimatedSizeInBytes is {}. ReportedSizeInBytes (actual reports from servers) is {}"
            + " and ReportedSizeInBytes per replica is {}",
        tableNameWithType, tableSubtypeSize._estimatedSizeInBytes, tableSubtypeSize._reportedSizeInBytes,
        tableSubtypeSize._reportedSizePerReplicaInBytes);

    emitStorageQuotaUtilizationMetric(tableNameWithType, tableSubtypeSize, allowedStorageBytesPerReplica);

    if (existingSegmentZkMetadata != null && tarSegmentSizeInBytes <= existingTarSegmentSize) {
      // If the segment already exists and the tarred size of the incoming segment is less than or equal to the
      // existing segment size, we can skip the quota check.
      String message = String.format(
          "Skipping storage quota check for segment %s of table %s since incoming tarred segment size %s is less than "
              + "or equal to existing segment size %s", segmentName, tableNameWithType,
          DataSizeUtils.fromBytes(tarSegmentSizeInBytes), DataSizeUtils.fromBytes(existingTarSegmentSize));
      LOGGER.info(message);
      return success(message);
    }

    long estimatedFinalSizeBytesPerReplica =
        tableSubtypeSize._reportedSizePerReplicaInBytes - existingSegmentSizeBytes + untarredSegmentSizeInBytes;
    if (estimatedFinalSizeBytesPerReplica <= allowedStorageBytesPerReplica) {
      String message = null;
      if (sizeDetails == null) {
        // append case
        message = String.format(
            "Appending Segment %s of Table %s is within quota. Total allowed storage size (per replica) : %s, "
                + "New estimated table size per replica: %s and number of replicas: %s."
                + " Current table size per replica: %s. "
                + "Incoming uncompressed segment size: %s. Formula: New estimated size = current table "
                + "size per replica + incoming segment size",
            segmentName, tableNameWithType,
            quotaConfig.getStorage(), DataSizeUtils.fromBytes(estimatedFinalSizeBytesPerReplica),
            numReplicas, DataSizeUtils.fromBytes(tableSubtypeSize._reportedSizePerReplicaInBytes),
            DataSizeUtils.fromBytes(untarredSegmentSizeInBytes));
      } else {
        // refresh case
        message = String.format(
            "Appending Segment %s of Table %s is within quota. Total allowed storage size (per replica) : %s, "
                + "New estimated table size per replica: %s and number of replicas: %s."
                + " Current table size per replica: %s. "
                + "Incoming uncompressed segment size: %s. Formula: New estimated size = current table "
                + "size per replica - existing same segment size + incoming segment size",
            segmentName, tableNameWithType,
            quotaConfig.getStorage(), DataSizeUtils.fromBytes(estimatedFinalSizeBytesPerReplica),
            numReplicas, DataSizeUtils.fromBytes(tableSubtypeSize._reportedSizePerReplicaInBytes),
            DataSizeUtils.fromBytes(untarredSegmentSizeInBytes));
      }

      LOGGER.info(message);
      return success(message);
    } else {
      String message;
      if (tableSubtypeSize._reportedSizePerReplicaInBytes > allowedStorageBytesPerReplica) {
        message = String.format(
            "Table %s already over quota and number of replicas: %s. Existing estimated uncompressed table size per "
                + "replica: %s > per replica allowed storage size: %s and Incoming uncompressed segment size: %s."
                + " Check if indexes were enabled recently and adjust table quota accordingly.", tableNameWithType,
            numReplicas, DataSizeUtils.fromBytes(tableSubtypeSize._reportedSizePerReplicaInBytes),
            DataSizeUtils.fromBytes(allowedStorageBytesPerReplica),
            DataSizeUtils.fromBytes(untarredSegmentSizeInBytes));
      } else {
        message = String.format(
            "Storage quota exceeded for Table %s and number of replicas: %s. New estimated size (per replica): %s "
                + "> allowed storage size (per replica): %s and Incoming uncompressed segment size: %s",
            tableNameWithType, numReplicas,
            DataSizeUtils.fromBytes(estimatedFinalSizeBytesPerReplica),
            DataSizeUtils.fromBytes(allowedStorageBytesPerReplica),
            DataSizeUtils.fromBytes(untarredSegmentSizeInBytes));
      }
      LOGGER.warn(message);
      return failure(message);
    }
  }

  private void emitStorageQuotaUtilizationMetric(String tableNameWithType, TableSizeReader.TableSubTypeSizeDetails
      tableSubtypeSize, long allowedStorageBytes) {
    // Only emit the real percentage of storage quota usage by lead controller, otherwise emit 0L.
    if (_leadControllerManager.isLeaderForTable(tableNameWithType)) {
      long existingStorageQuotaUtilization =
          tableSubtypeSize._reportedSizePerReplicaInBytes * 100 / allowedStorageBytes;
      _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.TABLE_STORAGE_QUOTA_UTILIZATION,
          existingStorageQuotaUtilization);
    } else {
      _controllerMetrics.setValueOfTableGauge(tableNameWithType,
          ControllerGauge.TABLE_STORAGE_QUOTA_UTILIZATION, 0L);
    }
  }

  /**
   * Checks whether the table is within the storage quota. This is used by the
   * {@link org.apache.pinot.controller.validation.RealtimeSegmentValidationManager} flow to decide whether realtime
   * consumption should be paused. It delegates to {@link #isSegmentStorageWithinQuota} with a {@code null} segment,
   * which triggers the table-level quota check (per-replica reported size vs configured per-replica quota, with
   * per-server retries on the table size read).
   * @return true if storage quota is exceeded by the table, else false.
   */
  public boolean isTableStorageQuotaExceeded(TableConfig tableConfig) {
    try {
      return !isSegmentStorageWithinQuota(tableConfig, null, 0, 0)._isSegmentWithinQuota;
    } catch (InvalidConfigException e) {
      // skip the check upon exception
      return false;
    }
  }
}
