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
package org.apache.pinot.segment.spi.crypt;

import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.spi.config.table.TableConfig;

/**
 * Objects implementing this interface
 * - Maintain a cache that maps KeyLineageUrns to a pre-constructed crypter object that can be invoked to decrypt
 *   a column value.
 * - Provide a method to update the cache while a segment is being loaded.
 * - Provide a method to get a crypter for a specific key lineage urn.
 *
 * @note: We dont have a method to call when segments are unloaded. This is on purpose. At least initially,
 * it is ok to just restart the servers. Over time (and depending on usage of this feature), we may need methots
 * to clear the cache, etc.
 * @param <T> represents the types of crypter objects that can be returned by getCrypter.
 */
public interface KeyBasedCrypterCache<T> {
    /**
     * @param tableConfig so that the cache can pick up the column names, encryption type, etc.
     */
    void init(TableConfig tableConfig);

    /**
     *
     * @param keyLineageUrn is the KeyLieageUrn returned by the KMS when encrytion key is created.
     * @return the crypter. The type of the crypter is TBD, but it should be able to decrypt any encrypted column of
     *          that row.
     */
    T getCrypter(String keyLineageUrn);

    /**
     * Segments can be loaded in parallel. In general, when this method returns, it implies that the all the values
     * of encryptionKeyColumnName have either been added to the cache, or in the process of being added (by a different
     * segment in another thread). i.e. if a different segment is loading and is handling a specific value of
     * encryptionKeyColumnName, then this method will return and not do the duplicate work of adding the value to the
     * cache. It will assume that the other segment will add the value.
     *
     * TODO Consider if blocking the segment load will be any better.
     *
     * @param segment A segment that is being loaded or replaced. The data is available in the segment.
     */
    void fillSegmentCache(ImmutableSegment segment);
}
