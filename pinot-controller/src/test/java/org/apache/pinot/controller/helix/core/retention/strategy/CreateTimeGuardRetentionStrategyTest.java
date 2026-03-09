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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class CreateTimeGuardRetentionStrategyTest {

  @Test
  public void testCreateTimeGuard() {
    String tableNameWithType = "myTable_OFFLINE";
    Clock nowClock = Clock.fixed(
        Instant.parse("2026-01-03T00:00:00Z"),
        ZoneOffset.UTC
    );

    TimeRetentionStrategy timeRetentionStrategy = new TimeRetentionStrategy(TimeUnit.DAYS, 30L);
    CreateTimeGuardRetentionStrategy retentionStrategy =
        new CreateTimeGuardRetentionStrategy(
            timeRetentionStrategy,
            Duration.ofHours(24).toMillis(),
            nowClock::millis
        );

    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata("mySegment");
    segmentZKMetadata.setTimeUnit(TimeUnit.DAYS);
    long today = TimeUnit.MILLISECONDS.toDays(System.currentTimeMillis());

    // creationTime = -1 (unset), endTime beyond retention — delegate returns true, guard skipped
    segmentZKMetadata.setEndTime(today - 60);
    assertTrue(retentionStrategy.isPurgeable(tableNameWithType, segmentZKMetadata));
    assertEquals(retentionStrategy.getSegmentsSkippedPurgeByCreateTime().size(), 0);

    // creationTime exactly at the guard boundary (24 h), endTime beyond retention
    // delegate returns true, guard fires (<=) — NOT purgeable
    segmentZKMetadata.setCreationTime(Instant.parse("2026-01-02T00:00:00Z").toEpochMilli());
    assertFalse(retentionStrategy.isPurgeable(tableNameWithType, segmentZKMetadata));
    assertEquals(retentionStrategy.getSegmentsSkippedPurgeByCreateTime().size(), 1);

    // creationTime within 24 h, endTime beyond retention
    // delegate returns true, guard fires — NOT purgeable
    segmentZKMetadata.setCreationTime(Instant.parse("2026-01-02T00:00:01Z").toEpochMilli());
    assertFalse(retentionStrategy.isPurgeable(tableNameWithType, segmentZKMetadata));
    assertEquals(retentionStrategy.getSegmentsSkippedPurgeByCreateTime().size(), 2);

    // creationTime beyond 24 h, endTime beyond retention
    // delegate returns true, guard does not fire — purgeable
    segmentZKMetadata.setCreationTime(Instant.parse("2026-01-01T23:59:59Z").toEpochMilli());
    assertTrue(retentionStrategy.isPurgeable(tableNameWithType, segmentZKMetadata));
    assertEquals(retentionStrategy.getSegmentsSkippedPurgeByCreateTime().size(), 2);

    // creationTime within 24 h, endTime within retention (5 days ago, within 30-day window)
    // delegate returns false — guard never runs, counter stays unchanged
    segmentZKMetadata.setEndTime(today - 5);
    segmentZKMetadata.setCreationTime(Instant.parse("2026-01-02T12:00:00Z").toEpochMilli());
    assertFalse(retentionStrategy.isPurgeable(tableNameWithType, segmentZKMetadata));
    assertEquals(retentionStrategy.getSegmentsSkippedPurgeByCreateTime().size(), 2);
  }

  @Test
  public void testCreateTimeGuardWhenCreateTimeGuardIsDisabled() {
    String tableNameWithType = "myTable_OFFLINE";
    Clock nowClock = Clock.fixed(
        Instant.parse("2026-01-03T00:00:00Z"),
        ZoneOffset.UTC
    );

    TimeRetentionStrategy timeRetentionStrategy = new TimeRetentionStrategy(TimeUnit.DAYS, 30L);
    CreateTimeGuardRetentionStrategy retentionStrategy =
        new CreateTimeGuardRetentionStrategy(
            timeRetentionStrategy,
            -1L, // disable create time guard
            nowClock::millis
        );

    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata("mySegment");
    segmentZKMetadata.setTimeUnit(TimeUnit.DAYS);
    long today = TimeUnit.MILLISECONDS.toDays(System.currentTimeMillis());
    segmentZKMetadata.setEndTime(today - 60);
    // create time is -1, should return true as per timeRetentionStrategy isPurgeable logic
    assertTrue(retentionStrategy.isPurgeable(tableNameWithType, segmentZKMetadata));

    // creation time is exactly 24 hours
    segmentZKMetadata.setCreationTime(Instant.parse("2026-01-02T00:00:00Z").toEpochMilli());
    assertTrue(retentionStrategy.isPurgeable(tableNameWithType, segmentZKMetadata));
    assertEquals(retentionStrategy.getSegmentsSkippedPurgeByCreateTime().size(), 0);

    // creation time is within 24 hours
    segmentZKMetadata.setCreationTime(Instant.parse("2026-01-02T00:00:01Z").toEpochMilli());
    assertTrue(retentionStrategy.isPurgeable(tableNameWithType, segmentZKMetadata));
    assertEquals(retentionStrategy.getSegmentsSkippedPurgeByCreateTime().size(), 0);
    // creation time is beyond 24 hours
    segmentZKMetadata.setCreationTime(Instant.parse("2026-01-01T23:59:59Z").toEpochMilli());
    // should return true as per timeRetentionStrategy isPurgeable logic
    assertTrue(retentionStrategy.isPurgeable(tableNameWithType, segmentZKMetadata));
  }
}
