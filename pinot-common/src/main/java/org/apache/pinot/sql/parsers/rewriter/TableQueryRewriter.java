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
package org.apache.pinot.sql.parsers.rewriter;

import org.apache.pinot.spi.config.table.TableConfig;

/**
 * This interface extends the QueryRewriter interface and is used to rewrite queries specific tables.
 * Rewriters implementing this interface get notified when a table is added in the broker so that they
 * can store state from the tableConfig and apply the appropriate rewriting functionality.
 */
public interface TableQueryRewriter extends QueryRewriter {
    /**
     * Register any new table that a broker instance has started to serve.
     * @param tableConfig
     */
    default void registerTable(TableConfig tableConfig) {
    }

    default void deregisterTable(TableConfig tableConfig) {
    }
}
