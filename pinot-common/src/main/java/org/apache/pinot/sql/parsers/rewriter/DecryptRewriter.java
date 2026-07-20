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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.config.EarInfo;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DecryptRewriter implements TableQueryRewriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(DecryptRewriter.class);

  private final static Map<String, EarInfo> TABLE_2_EAR_INFO = new ConcurrentHashMap<>();
  private final static String DECRYPT_DOUBLE = "decryptDouble";
  private final static String DECRYPT_LONG = "decryptLong";
  private final static Set<String> DECRYPT_FUNCTIONS = Set.of(DECRYPT_DOUBLE, DECRYPT_LONG);
  private final static Map<FieldSpec.DataType, String> DATA_TYPE_2_DECRYPT_FUNCTION = Map.of(
          FieldSpec.DataType.DOUBLE, DECRYPT_DOUBLE,
          FieldSpec.DataType.LONG, DECRYPT_LONG
  );

  @Override
  public void registerTable(TableConfig tableConfig) {
    EarInfo earInfo = new EarInfo(tableConfig);
    if (!earInfo.isEarEnabled()) {
      return;
    }
    String tableName = TableNameBuilder.extractRawTableName(tableConfig.getTableName());
    EarInfo oldEarInfo = TABLE_2_EAR_INFO.putIfAbsent(tableName, earInfo);
    if (oldEarInfo != null) {
      LOGGER.warn("Table {} is already registered for EAR. Changing EAR parameters not allowed", tableName);
      return;
    }
  }

  @Override
  public void deregisterTable(TableConfig tableConfig) {
    // The broker invokes this during an ONLINE -> DROPPED state transition, where the table config fetched from the
    // property store may already be gone (null) because the table is being dropped. Guard against a null config (and a
    // null table name) so deregistration is a no-op instead of throwing an NPE and failing the state transition.
    if (tableConfig == null || tableConfig.getTableName() == null) {
      return;
    }
    String tableName = TableNameBuilder.extractRawTableName(tableConfig.getTableName());
    TABLE_2_EAR_INFO.remove(tableName);
  }

  @Override
  public PinotQuery rewrite(PinotQuery query) {
    // Rewrite select
    if (query.getDataSource() == null) {
      return query;
    }
    if (query.getDataSource().getJoin() != null) {
      // JOIN not supported as yet
      return query;
    }
    String tableName = query.getDataSource().getTableName();
    if (tableName == null) {
      return query;
    }
    EarInfo earInfo = TABLE_2_EAR_INFO.get(tableName);
    if (earInfo == null || !earInfo.isEarEnabled()) {
      return query;
    }

    List<Expression> oldSelectList = query.getSelectList();
    List<Expression> newSelectList = new ArrayList<>();
    for (Expression expr: oldSelectList) {
      newSelectList.add(rewriteExpression(earInfo, expr, true));
    }
    query.setSelectList(newSelectList);

    // Rewrite FILTER
    query.setFilterExpression(rewriteExpression(earInfo, query.getFilterExpression(), false));

    // Handle group by
    List<Expression> oldGroupByList = query.getGroupByList();
    if (oldGroupByList != null) {
      List<Expression> newGroupByList = new ArrayList<>();
      for (Expression expr: oldGroupByList) {
        newGroupByList.add(rewriteExpression(earInfo, expr, false));
      }
      query.setGroupByList(newGroupByList);
    }

    // Handle Order by
    List<Expression> oldOrderbyList = query.getOrderByList();
    if (oldOrderbyList != null) {
      List<Expression> newOrderbyList = new ArrayList<>();
      for (Expression expr: oldOrderbyList) {
        newOrderbyList.add(rewriteExpression(earInfo, expr, false));
      }
      query.setOrderByList(newOrderbyList);
    }
    return query;
  }

  private Expression rewriteExpression(EarInfo earInfo, Expression expr, boolean isSelection) {
    if (expr == null) {
      return null;
    }

    switch (expr.getType()) {
      case IDENTIFIER:
        String col = expr.getIdentifier().getName();
        if (earInfo.getEarEncryptedColumns().containsKey(col)) {
          return makeDecryptExpression(earInfo, col, isSelection);
        }
        return expr;

      case FUNCTION:
        if (DECRYPT_FUNCTIONS.contains(expr.getFunctionCall().getOperator())) {
          return expr;
        }
        List<Expression> oldOperands = expr.getFunctionCall().getOperands();
        if (oldOperands != null) {
          List<Expression> newOperands = new ArrayList<>();
          for (Expression expression: oldOperands) {
            newOperands.add(rewriteExpression(earInfo, expression, false));
          }
          expr.getFunctionCall().setOperands(newOperands);
        }
        return expr;

      default:
        return expr;
    }
  }

  private static Expression makeDecryptExpression(EarInfo earInfo, String targetCol, boolean isSelection) {
    String kluCol = earInfo.getEarKLUColumn();
    Function fn = new Function();
    fn.setOperator(DATA_TYPE_2_DECRYPT_FUNCTION.get(earInfo.getEarEncryptedColumns().get(targetCol)));
    fn.addToOperands(makeIdentifier(kluCol));
    fn.addToOperands(makeIdentifier(targetCol));

    Expression expr = new Expression();
    expr.setType(ExpressionType.FUNCTION);
    expr.setFunctionCall(fn);

    if (!isSelection) {
      return expr;
    }
    Expression aliasExpr = new Expression();
    aliasExpr.setFunctionCall(new Function());
    aliasExpr.setType(ExpressionType.FUNCTION);
    aliasExpr.getFunctionCall().setOperator("as");
    aliasExpr.getFunctionCall().addToOperands(expr);
    aliasExpr.getFunctionCall().addToOperands(makeIdentifier(targetCol));
    return aliasExpr;
  }

  private static Expression makeIdentifier(String name) {
    Expression expr = new Expression();
    Identifier id = new Identifier();
    id.setName(name);
    expr.setType(ExpressionType.IDENTIFIER);
    expr.setIdentifier(id);
    return expr;
  }
}
