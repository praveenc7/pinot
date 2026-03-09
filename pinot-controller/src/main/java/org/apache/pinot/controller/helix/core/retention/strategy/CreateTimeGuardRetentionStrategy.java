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
package org.apache.pinot.controller.helix.core.retention.strategy;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A decorator around {@link RetentionStrategy} that adds a creation-time safety guard.
 * The delegate strategy is evaluated first; if the delegate decides the segment is purgeable,
 * the guard checks whether the segment was created within the last {@code maxAgeMs} milliseconds
 * and, if so, overrides the decision and keeps the segment.
 *
 * <p>The guard is skipped when:
 * <ul>
 *   <li>The delegate strategy returns {@code false} (segment is not purgeable).</li>
 *   <li>The segment's creation time is unset (reported as {@code -1}).</li>
 *   <li>{@code maxAgeMs} is {@code <= 0}, which disables the guard entirely.</li>
 * </ul>
 */
public class CreateTimeGuardRetentionStrategy implements RetentionStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(CreateTimeGuardRetentionStrategy.class);

  private final RetentionStrategy _delegate;
  private final long _maxAgeMs;
  // timeSupplier is used to get the current time in milliseconds.
  private Supplier<Long> _timeSupplier;
  private List<String> _segmentsSkippedPurgeByCreateTime = new ArrayList<>();

  public CreateTimeGuardRetentionStrategy(RetentionStrategy delegate, long maxAgeMs, Supplier<Long> timeSupplier) {
    _delegate = delegate;
    _maxAgeMs = maxAgeMs;
    _timeSupplier = timeSupplier;
  }

  /**
   * Determines whether a given segment is eligible for deletion (purge).
   *
   * <p>This method first delegates to the underlying {@code _delegate.isPurgeable} to determine
   * baseline purgeability. Then, it applies additional constraints based on creation time and
   * maximum age:
   *
   * <ul>
   *   <li>If the segment's creation time is unavailable (<= 0), the delegate's result is used.
   *   <li>If a {@code _maxAgeMs} is set and the segment's age is less than this threshold, the
   *       segment will <b>not</b> be purged, and a warning log is emitted showing both the
   *       creation time and current age.
   * </ul>
   *
   * <p>The number of segments skipped due to age checks is incremented in
   * {@code _numSegmentsSkippedPurgeByCreateTime}.
   *
   * @param tableNameWithType the fully qualified table name (including type) for the segment
   * @param segmentZKMetadata metadata for the segment, including its creation time
   * @return {@code true} if the segment can be purged; {@code false} if it is skipped due to
   *         age constraints or delegate result
   */
  @Override
  public boolean isPurgeable(String tableNameWithType, SegmentZKMetadata segmentZKMetadata) {
    boolean purgeable = _delegate.isPurgeable(tableNameWithType, segmentZKMetadata);
    if (!purgeable) {
      return false;
    }

    long creationTime = segmentZKMetadata.getCreationTime();
    if (creationTime <= 0) {
      return true;
    }

    long now = _timeSupplier.get();
    long ageMs = now - creationTime;
    if (_maxAgeMs > 0 && ageMs <= _maxAgeMs) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Skipping deletion for segment: {} of table: {}. CreationTime(ms): {}, Age(ms): {}, MaxAge(ms): {}",
            segmentZKMetadata.getSegmentName(),
            tableNameWithType,
            creationTime,
            ageMs,
            _maxAgeMs
        );
      }

      _segmentsSkippedPurgeByCreateTime.add(segmentZKMetadata.getSegmentName());
      return false;
    }

    return true;
  }

  @Override
  public boolean isPurgeable(String tableNameWithType, String segmentName, long segmentTimeMs) {
    return _delegate.isPurgeable(tableNameWithType, segmentName, segmentTimeMs);
  }

  /**
   * Returns the segments that were skipped for purging due to the creation time guard.
   * Only collect the segments when the delegate decided to purge but the guard overrode that decision.
   *
   * @return segments actively protected by the creation-time guard
   */
  public List<String> getSegmentsSkippedPurgeByCreateTime() {
    return _segmentsSkippedPurgeByCreateTime;
  }
}
