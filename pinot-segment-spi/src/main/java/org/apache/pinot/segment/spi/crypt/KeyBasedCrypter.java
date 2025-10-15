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

/**
 * All the encryption algorithms we support need to implement this interface.
 */
public interface KeyBasedCrypter {
    /**
     * Decrypt the input cypher-text into a byte array.
     * @param cypherText
     * @return
     */
    byte[] decrypt(byte[] cypherText);

    /**
     * This method is needed for backward compatibility only. It will be removed in the future.
     * @param cypherText
     * @return a double value
     */
    default double decryptToDouble(String cypherText) {
        throw new UnsupportedOperationException("Not implemented");
    };

    /**
     *
     * This method is needed for backward compatibility only. It will be removed in the future.
     * @param cypherText
     * @return a long value
     */
    default long decryptToLong(String cypherText) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
