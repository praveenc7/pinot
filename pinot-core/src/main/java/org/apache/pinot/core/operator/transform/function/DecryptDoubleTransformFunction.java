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
package org.apache.pinot.core.operator.transform.function;

import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.RowLevelDecrypter;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This transform function decrypts an encrypted double value using the key lineage URN from the KLU column.
 */
public class DecryptDoubleTransformFunction extends BaseTransformFunction {

    private static final Logger LOGGER = LoggerFactory.getLogger(DecryptDoubleTransformFunction.class);
    public static final String FUNCTION_NAME = "decryptDouble";
    private TransformFunction _firstTransformFunction;
    private TransformFunction _secondTransformFunction;
    private RowLevelDecrypter _decrypter;
    // Until such time as Pinot builds the capability of encryption during data ingestion, we support
    // encrypted values as either string or bytes. This flag is used to determine which type of value
    // is being used.
    // Current usage is for encrypted values to be stored as Base64 encoded strings.
    private boolean _isEncryptedValueString;

    @Override
    public String getName() {
        return FUNCTION_NAME;
    }

    @Override
    // Argument order:  KLU column name followed by encrypted value column name (assume string)
    public void init(List<TransformFunction> arguments,
                     Map<String, ColumnContext> columnContextMap,
                     QueryContext queryContext) {
        super.init(arguments, columnContextMap);
        // Check that there are exactly 2 arguments
        if (arguments.size() != 2) {
            throw new IllegalArgumentException("Exactly 2 arguments are required for DECRYPT transform function");
        }

        for (int i = 0; i < arguments.size(); i++) {
            TransformFunction argument = arguments.get(i);
            if (!argument.getResultMetadata().isSingleValue()) {
                throw new IllegalArgumentException("every argument of DECRYPT transform function must be"
                        + " single-valued");
            }
            if (i == 0) {
                if (!argument.getResultMetadata().getDataType().equals(FieldSpec.DataType.STRING)) {
                    throw new IllegalArgumentException("First argument should be of type " + FieldSpec.DataType.STRING);
                }
                _firstTransformFunction = argument;
            } else {
                if (argument.getResultMetadata().getDataType().equals(FieldSpec.DataType.STRING)) {
                    _isEncryptedValueString = true;
                } else if (argument.getResultMetadata().getDataType().equals(FieldSpec.DataType.BYTES)) {
                    _isEncryptedValueString = false;
                } else {
                    throw new IllegalArgumentException("Second argument should be of type "
                            + FieldSpec.DataType.BYTES + " or " + FieldSpec.DataType.STRING);
                }
                _secondTransformFunction = argument;
            }
        }
        _decrypter = new RowLevelDecrypter(queryContext.getCrypterCache());
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
        return DOUBLE_SV_NO_DICTIONARY_METADATA;
    }

    @SuppressWarnings("Duplicates")
    @Override
    public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
        int length = valueBlock.getNumDocs();
        initDoubleValuesSV(length);
        // Use the values of the first transform function to get the key to fetch decryption key
        // decrypt the second transform function values using the decryption key
        String[] klus = _firstTransformFunction.transformToStringValuesSV(valueBlock);
        if (_isEncryptedValueString) {
            String[] encryptedValues = _secondTransformFunction.transformToStringValuesSV(valueBlock);
            for (int i = 0; i < length; i++) {
                _doubleValuesSV[i] = _decrypter.decryptToDouble(klus[i], encryptedValues[i]);
            }
        } else {
            byte[][] encryptedValues = _secondTransformFunction.transformToBytesValuesSV(valueBlock);
            for (int i = 0; i < length; i++) {
                _doubleValuesSV[i] = _decrypter.decryptToDouble(klus[i], encryptedValues[i]);
            }
        }
        return _doubleValuesSV;
    }
}
