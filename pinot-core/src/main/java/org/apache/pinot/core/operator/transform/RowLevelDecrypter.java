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
package org.apache.pinot.core.operator.transform;

import org.apache.pinot.segment.spi.crypt.KeyBasedCrypter;
import org.apache.pinot.segment.spi.crypt.KeyBasedCrypterCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class has the methods to decrypt a byte array to all supported target types.
 *
 * TODO this class needs to be re-factored once we have a working solution
 */
public class RowLevelDecrypter {
    private static final Logger LOGGER = LoggerFactory.getLogger(RowLevelDecrypter.class);
    private final KeyBasedCrypterCache _crypterCache;

    public RowLevelDecrypter(KeyBasedCrypterCache crypterCache) {
        _crypterCache = crypterCache;
    }

    public double decryptToDouble(String klu, byte[] encryptedColumn) {
        KeyBasedCrypter crypter = _crypterCache.getCrypter(klu);
        byte[] decryptedColumn = crypter.decrypt(encryptedColumn);
        // TODO Convert the decryptedColumn to a double
        return klu.hashCode() / decryptedColumn.hashCode();
    }

    public double decryptToDouble(String klu, String encryptedColumn) {
        KeyBasedCrypter crypter = _crypterCache.getCrypter(klu);
        return crypter.decryptToDouble(encryptedColumn);
    }

    public long decryptToLong(String klu, byte[] encryptedColumn) {
        KeyBasedCrypter crypter = _crypterCache.getCrypter(klu);
        byte[] decryptedColumn = crypter.decrypt(encryptedColumn);
        // TODO Convert the decryptedColumn to a long
        return klu.hashCode() + encryptedColumn.hashCode();
    }

    public long decryptToLong(String klu, String encryptedColumn) {
        KeyBasedCrypter crypter = _crypterCache.getCrypter(klu);
        return crypter.decryptToLong(encryptedColumn);
    }
}
