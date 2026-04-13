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
package org.apache.pinot.segment.spi.index;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.IndexConfig;


/**
 * Configuration for the bitmap inverted index, including the file format version to use when writing new segments.
 *
 * <p>The version controls the on-disk file format written by the creator:
 * <ul>
 *   <li>Version 0 (default): no file header, 32-bit offsets. Legacy format, limited to &lt; 4 GB index files.</li>
 *   <li>Version 1: magic-number + version header, 64-bit offsets. Supports segments larger than 4 GB.</li>
 * </ul>
 *
 * <p>Readers always auto-detect the format from the file itself, so existing version-0 segments remain readable
 * regardless of the configured version.
 */
public class InvertedIndexConfig extends IndexConfig {
  /** Legacy format: no file header, 32-bit offsets. */
  public static final int VERSION_0 = 0;
  /** Versioned format: magic-number + version header, 64-bit offsets. */
  public static final int VERSION_1 = 1;
  /** The version written by default when no explicit version is configured. */
  public static final int DEFAULT_VERSION = VERSION_0;
  /** Enabled with {@link #DEFAULT_VERSION}. Consistent with {@link IndexConfig#ENABLED}. */
  public static final InvertedIndexConfig ENABLED = new InvertedIndexConfig(false, null);
  public static final InvertedIndexConfig DISABLED = new InvertedIndexConfig(true, null);

  private final int _version;

  public InvertedIndexConfig(int version) {
    this(false, version);
  }

  @JsonCreator
  public InvertedIndexConfig(@JsonProperty("disabled") Boolean disabled,
      @JsonProperty("version") @Nullable Integer version) {
    super(disabled);
    if (version != null) {
      Preconditions.checkArgument(version == VERSION_0 || version == VERSION_1,
          "Unsupported inverted index version: %s. Valid versions are %s (legacy) and %s (v1 with 64-bit offsets)",
          version, VERSION_0, VERSION_1);
      _version = version;
    } else {
      _version = DEFAULT_VERSION;
    }
  }

  public int getVersion() {
    return _version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    InvertedIndexConfig that = (InvertedIndexConfig) o;
    return _version == that._version;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _version);
  }
}
