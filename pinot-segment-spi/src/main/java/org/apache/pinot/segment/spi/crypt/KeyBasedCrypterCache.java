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

import java.util.List;
import org.apache.pinot.segment.spi.ImmutableSegment;

public interface KeyBasedCrypterCache<K, V> {
    /**
     * TODO include the encryption scheme (could be different for each encrypted column)
     * @param encryptionKeyColumnName
     * @param encryptedColumnNames
     */
    void init(String encryptionKeyColumnName, List<String> encryptedColumnNames);

    /**
     *
     * @param key
     * @return
     */
    V getCrypter(K key);

    /**
     * Segments can be loaded in parallel. In general, when this method returns, it implies that the all the values
     * of encryptionKeyColumnName have either been added to the cache, or in the process of being added (by a different
     * segment in another thread). i.e. if a different segment is loading and is handling a specific value of
     * encryptionKeyColumnName, then this method will return and not do the duplicate work of adding the value to the
     * cache. It will assume that the other segment will add the value.
     *
     * TODO Consider if blocking the segment load will be any better.
     *
     * @param segment
     */
    void fillSegmentCache(ImmutableSegment segment);
}
