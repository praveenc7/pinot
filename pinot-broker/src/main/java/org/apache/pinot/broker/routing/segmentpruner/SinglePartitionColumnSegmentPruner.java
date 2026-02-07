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
package org.apache.pinot.broker.routing.segmentpruner;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.segmentpartition.SegmentPartitionInfo;
import org.apache.pinot.broker.routing.segmentpartition.SegmentPartitionUtils;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.sql.FilterKind;


/**
 * The {@code SinglePartitionColumnSegmentPruner} prunes segments based on their partition metadata stored in ZK. The
 * pruner supports queries with filter (or nested filter) of EQUALITY and IN predicates.
 */
public class SinglePartitionColumnSegmentPruner implements SegmentPruner {
  private final String _tableNameWithType;
  private final String _partitionColumn;
  private final Map<String, SegmentPartitionInfo> _partitionInfoMap = new ConcurrentHashMap<>();

  public SinglePartitionColumnSegmentPruner(String tableNameWithType, String partitionColumn) {
    _tableNameWithType = tableNameWithType;
    _partitionColumn = partitionColumn;
  }

  @Override
  public void init(IdealState idealState, ExternalView externalView, List<String> onlineSegments,
      List<ZNRecord> znRecords) {
    // Bulk load partition info for all online segments
    for (int idx = 0; idx < onlineSegments.size(); idx++) {
      String segment = onlineSegments.get(idx);
      SegmentPartitionInfo partitionInfo =
          SegmentPartitionUtils.extractPartitionInfo(_tableNameWithType, _partitionColumn, segment, znRecords.get(idx));
      if (partitionInfo != null) {
        _partitionInfoMap.put(segment, partitionInfo);
      }
    }
  }

  @Override
  public synchronized void onAssignmentChange(IdealState idealState, ExternalView externalView,
      Set<String> onlineSegments, List<String> pulledSegments, List<ZNRecord> znRecords) {
    // NOTE: We don't update all the segment ZK metadata for every external view change, but only the new added/removed
    //       ones. The refreshed segment ZK metadata change won't be picked up.
    for (int idx = 0; idx < pulledSegments.size(); idx++) {
      String segment = pulledSegments.get(idx);
      ZNRecord znRecord = znRecords.get(idx);
      _partitionInfoMap.computeIfAbsent(segment,
          k -> SegmentPartitionUtils.extractPartitionInfo(_tableNameWithType, _partitionColumn, k, znRecord));
    }
    _partitionInfoMap.keySet().retainAll(onlineSegments);
  }

  @Override
  public synchronized void refreshSegment(String segment, @Nullable ZNRecord znRecord) {
    SegmentPartitionInfo partitionInfo =
        SegmentPartitionUtils.extractPartitionInfo(_tableNameWithType, _partitionColumn, segment, znRecord);
    if (partitionInfo != null) {
      _partitionInfoMap.put(segment, partitionInfo);
    } else {
      _partitionInfoMap.remove(segment);
    }
  }

  @Override
  public Set<String> prune(BrokerRequest brokerRequest, Set<String> segments) {
    Expression filterExpression = brokerRequest.getPinotQuery().getFilterExpression();
    if (filterExpression == null) {
      return segments;
    }

    // Cache of partition function key -> query partition IDs
    // This avoids recomputing partition IDs for each segment when they share the same partition function
    Map<String, Set<Integer>> queryPartitionIdsByFunction = new HashMap<>();

    Set<String> selectedSegments = new HashSet<>();
    for (String segment : segments) {
      SegmentPartitionInfo partitionInfo = _partitionInfoMap.get(segment);
      if (partitionInfo == null || partitionInfo == SegmentPartitionUtils.INVALID_PARTITION_INFO) {
        // No partition info available, include segment to be safe
        selectedSegments.add(segment);
        continue;
      }

      PartitionFunction partitionFunction = partitionInfo.getPartitionFunction();
      String partitionFunctionKey = partitionFunction.getPartitionFunctionKey();

      // Get or compute the query partition IDs for this partition function
      Set<Integer> queryPartitionIds = queryPartitionIdsByFunction.computeIfAbsent(
          partitionFunctionKey,
          k -> extractPartitionIds(filterExpression, partitionFunction)
      );

      // null means the filter doesn't constrain the partition column, so all segments match
      // Otherwise, check if segment's partitions intersect with query's partition IDs
      if (queryPartitionIds == null || hasIntersection(partitionInfo.getPartitions(), queryPartitionIds)) {
        selectedSegments.add(segment);
      }
    }
    return selectedSegments;
  }

  /**
   * Extracts the set of partition IDs from the filter expression that match the partition column.
   *
   * @param filterExpression The filter expression to analyze
   * @param partitionFunction The partition function to use for computing partition IDs
   * @return Set of partition IDs that match the filter, or null if the filter doesn't constrain
   *         the partition column (meaning all partitions match)
   */
  @Nullable
  private Set<Integer> extractPartitionIds(Expression filterExpression, PartitionFunction partitionFunction) {
    Function function = filterExpression.getFunctionCall();
    FilterKind filterKind = FilterKind.valueOf(function.getOperator());
    List<Expression> operands = function.getOperands();

    switch (filterKind) {
      case AND: {
        // For AND: intersection of partition sets from children
        // If any child returns empty set, result is empty
        // If all children return null, result is null
        Set<Integer> result = null;
        for (Expression child : operands) {
          Set<Integer> childPartitions = extractPartitionIds(child, partitionFunction);
          if (childPartitions != null) {
            if (childPartitions.isEmpty()) {
              // Short-circuit: empty set AND anything = empty set
              return Collections.emptySet();
            }
            if (result == null) {
              result = new HashSet<>(childPartitions);
            } else {
              result.retainAll(childPartitions);
              if (result.isEmpty()) {
                return Collections.emptySet();
              }
            }
          }
        }
        return result;
      }
      case OR: {
        // For OR: union of partition sets from children
        // If any child returns null, result is null (that child matches all partitions)
        Set<Integer> result = new HashSet<>();
        for (Expression child : operands) {
          Set<Integer> childPartitions = extractPartitionIds(child, partitionFunction);
          if (childPartitions == null) {
            // Short-circuit: null (all partitions) OR anything = null (all partitions)
            return null;
          }
          result.addAll(childPartitions);
        }
        return result;
      }
      case EQUALS: {
        Identifier identifier = operands.get(0).getIdentifier();
        if (identifier != null && identifier.getName().equals(_partitionColumn)) {
          int partitionId = partitionFunction.getPartition(RequestContextUtils.getStringValue(operands.get(1)));
          return Collections.singleton(partitionId);
        }
        // Not on partition column, doesn't constrain partitions
        return null;
      }
      case IN: {
        Identifier identifier = operands.get(0).getIdentifier();
        if (identifier != null && identifier.getName().equals(_partitionColumn)) {
          Set<Integer> partitionIds = new HashSet<>();
          int numOperands = operands.size();
          for (int i = 1; i < numOperands; i++) {
            partitionIds.add(partitionFunction.getPartition(RequestContextUtils.getStringValue(operands.get(i))));
          }
          return partitionIds;
        }
        // Not on partition column, doesn't constrain partitions
        return null;
      }
      default:
        // Other filter types don't constrain the partition column
        return null;
    }
  }

  /**
   * Checks if two sets have any common elements.
   */
  private static boolean hasIntersection(Set<Integer> set1, Set<Integer> set2) {
    return !Collections.disjoint(set1, set2);
  }
}
