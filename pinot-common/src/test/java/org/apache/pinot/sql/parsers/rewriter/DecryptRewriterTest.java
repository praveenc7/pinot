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

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.Join;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.config.EarInfo;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.mockito.MockedConstruction;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


public class DecryptRewriterTest {
  private DecryptRewriter _decryptRewriter;
  private static final String UNREGISTERED_TABLE = "testTableUnregistered";
  private static final String EAR_ENABLED_TABLE = "testTableEnabledEar";
  private static final String EAR_DISABLED_TABLE = "testTableDisabledEar";
  private static final String KLU_COLUMN = "klu_col";
  private static final String ENCRYPTED_DOUBLE_COLUMN = "encrypted_double_col";
  private static final String ENCRYPTED_LONG_COLUMN = "encrypted_long_col";
  private static final String NORMAL_COLUMN = "normal_col";
  private static final Map<String, FieldSpec.DataType> ENCRYPTED_COLS_AND_TYPES = Map.of(
          ENCRYPTED_DOUBLE_COLUMN, FieldSpec.DataType.DOUBLE,
          ENCRYPTED_LONG_COLUMN, FieldSpec.DataType.LONG
  );

  @BeforeTest
  public void setUp() throws Exception {
    _decryptRewriter = new DecryptRewriter();
    Field table2EarInfo = DecryptRewriter.class.getDeclaredField("TABLE_2_EAR_INFO");
    table2EarInfo.setAccessible(true);

    EarInfo enabledEar = mock(EarInfo.class);
    when(enabledEar.isEarEnabled()).thenReturn(true);
    when(enabledEar.getEarKLUColumn()).thenReturn(KLU_COLUMN);
    when(enabledEar.getEarEncryptedColumns()).thenReturn(ENCRYPTED_COLS_AND_TYPES);

    EarInfo disabledEar = mock(EarInfo.class);
    when(disabledEar.isEarEnabled()).thenReturn(false);
    when(disabledEar.getEarKLUColumn()).thenReturn(null);
    when(disabledEar.getEarEncryptedColumns()).thenReturn(null);

    @SuppressWarnings("unchecked")
    Map<String, EarInfo> map = (Map<String, EarInfo>) table2EarInfo.get(null);
    map.put(EAR_ENABLED_TABLE, enabledEar);
    map.put(EAR_DISABLED_TABLE, disabledEar);
  }

  @AfterTest
  public void tearDown() throws Exception {
  }

  private Expression createIdentifierExpression(String columnName) {
    Expression expr = new Expression();
    expr.setType(ExpressionType.IDENTIFIER);
    Identifier identifier = new Identifier();
    identifier.setName(columnName);
    expr.setIdentifier(identifier);
    return expr;
  }

  private Expression createFunctionExpression(String operator, List<Expression> operands) {
    Expression expr = new Expression();
    expr.setType(ExpressionType.FUNCTION);
    Function function = new Function();
    function.setOperator(operator);
    function.setOperands(operands);
    expr.setFunctionCall(function);
    return expr;
  }

  private void verifyDecryptDouble(Expression expression) {
    assertEquals(expression.getFunctionCall().getOperator(), "decryptDouble");
    Expression kluCol = expression.getFunctionCall().getOperands().get(0);
    assertEquals(kluCol.getIdentifier().getName(), KLU_COLUMN);
    Expression encryptedDoubleCol = expression.getFunctionCall().getOperands().get(1);
    assertEquals(encryptedDoubleCol.getIdentifier().getName(), ENCRYPTED_DOUBLE_COLUMN);
  }


  @Test
  public void testRewriteSelectClauseWithEncryptedDoubleColumn() throws Exception {
    // Setup the test with the mocked EarInfo

    // Create and execute query
    PinotQuery query = CalciteSqlParser.compileToPinotQueryWithoutRewrites(
        "SELECT " + ENCRYPTED_DOUBLE_COLUMN + " FROM " + EAR_ENABLED_TABLE);
    PinotQuery rewrittenQuery = _decryptRewriter.rewrite(query);

    // Verify the query was rewritten correctly
    assertNotNull(rewrittenQuery);
    assertEquals(rewrittenQuery.getSelectList().size(), 1);

    // Verify the select expression has the expected structure
    Expression selectExpr = rewrittenQuery.getSelectList().get(0);
    assertEquals(selectExpr.getType(), ExpressionType.FUNCTION);
    assertEquals(selectExpr.getFunctionCall().getOperator(), "as");

    // Verify the decrypt function call
    Expression decryptExpr = selectExpr.getFunctionCall().getOperands().get(0);
    assertEquals(decryptExpr.getFunctionCall().getOperator(), "decryptDouble");
    assertEquals(decryptExpr.getFunctionCall().getOperands().size(), 2);

    // Verify the function arguments
    assertEquals(decryptExpr.getFunctionCall().getOperands().get(0).getIdentifier().getName(), KLU_COLUMN);
    assertEquals(decryptExpr.getFunctionCall().getOperands().get(1).getIdentifier().getName(), ENCRYPTED_DOUBLE_COLUMN);

    // Verify the alias
    Expression aliasExpr = selectExpr.getFunctionCall().getOperands().get(1);
    assertEquals(aliasExpr.getIdentifier().getName(), ENCRYPTED_DOUBLE_COLUMN);
  }

  @Test
  public void testRewriteSelectClauseWithEncryptedLongColumn() throws Exception {
    // Setup the test with the mocked EarInfo

    // Create and execute query
    PinotQuery query = CalciteSqlParser.compileToPinotQueryWithoutRewrites(
        "SELECT " + ENCRYPTED_LONG_COLUMN + " FROM " + EAR_ENABLED_TABLE);
    PinotQuery rewrittenQuery = _decryptRewriter.rewrite(query);

    // Verify the query was rewritten correctly
    assertNotNull(rewrittenQuery);
    assertEquals(rewrittenQuery.getSelectList().size(), 1);

    // Verify the select expression has the expected structure
    Expression selectExpr = rewrittenQuery.getSelectList().get(0);
    assertEquals(selectExpr.getType(), ExpressionType.FUNCTION);
    assertEquals(selectExpr.getFunctionCall().getOperator(), "as");

    // Verify the decrypt function call
    Expression decryptExpr = selectExpr.getFunctionCall().getOperands().get(0);
    assertEquals(decryptExpr.getFunctionCall().getOperator(), "decryptLong");
    assertEquals(decryptExpr.getFunctionCall().getOperands().size(), 2);

    // Verify the function arguments
    assertEquals(decryptExpr.getFunctionCall().getOperands().get(0).getIdentifier().getName(), KLU_COLUMN);
    assertEquals(decryptExpr.getFunctionCall().getOperands().get(1).getIdentifier().getName(), ENCRYPTED_LONG_COLUMN);

    // Verify the alias
    Expression aliasExpr = selectExpr.getFunctionCall().getOperands().get(1);
    assertEquals(aliasExpr.getIdentifier().getName(), ENCRYPTED_LONG_COLUMN);
  }

  @Test
  public void testRewriteSelectClauseWithMixedColumns() throws Exception {
    PinotQuery query = CalciteSqlParser.compileToPinotQueryWithoutRewrites(
        "SELECT " + ENCRYPTED_DOUBLE_COLUMN + ", " + NORMAL_COLUMN + " FROM " + EAR_ENABLED_TABLE);

    PinotQuery rewrittenQuery = _decryptRewriter.rewrite(query);

    // First column should be rewritten with decrypt function and alias
    Expression encryptedSelectExpr = rewrittenQuery.getSelectList().get(0);
    assertEquals(encryptedSelectExpr.getType(), ExpressionType.FUNCTION);
    assertEquals(encryptedSelectExpr.getFunctionCall().getOperator(), "as");

    // Second column should remain unchanged
    Expression normalSelectExpr = rewrittenQuery.getSelectList().get(1);
    assertEquals(normalSelectExpr.getType(), ExpressionType.IDENTIFIER);
    assertEquals(normalSelectExpr.getIdentifier().getName(), NORMAL_COLUMN);
  }

  @Test
  public void testRewriteWhereClauseWithEncryptedColumn() throws Exception {

    PinotQuery query = CalciteSqlParser.compileToPinotQueryWithoutRewrites(
        "SELECT * FROM " + EAR_ENABLED_TABLE + " WHERE " + ENCRYPTED_DOUBLE_COLUMN + " > 100");

    PinotQuery rewrittenQuery = _decryptRewriter.rewrite(query);

    Expression filterExpr = rewrittenQuery.getFilterExpression();
    assertEquals(filterExpr.getFunctionCall().getOperator(), "GREATER_THAN");

    // Left operand should be decrypt function (without alias in WHERE clause)
    Expression leftOperand = filterExpr.getFunctionCall().getOperands().get(0);
    assertEquals(leftOperand.getFunctionCall().getOperator(), "decryptDouble");
    assertEquals(leftOperand.getFunctionCall().getOperands().size(), 2);

    assertEquals(leftOperand.getFunctionCall().getOperands().get(0).getIdentifier().getName(), KLU_COLUMN);
    assertEquals(leftOperand.getFunctionCall().getOperands().get(1).getIdentifier().getName(), ENCRYPTED_DOUBLE_COLUMN);
  }

  @Test
  public void testRewriteWhereClauseWithComplexFilter() throws Exception {
    PinotQuery query = CalciteSqlParser.compileToPinotQueryWithoutRewrites(
        "SELECT * FROM " + EAR_ENABLED_TABLE + " WHERE " + ENCRYPTED_DOUBLE_COLUMN + " > 100 AND " + NORMAL_COLUMN
                + " = 'test'");

    PinotQuery rewrittenQuery = _decryptRewriter.rewrite(query);

    Expression filterExpr = rewrittenQuery.getFilterExpression();
    assertEquals(filterExpr.getFunctionCall().getOperator(), "AND");

    // First condition should have decrypt function
    Expression leftCondition = filterExpr.getFunctionCall().getOperands().get(0);
    assertEquals(leftCondition.getFunctionCall().getOperator(), "GREATER_THAN");
    Expression leftOperand = leftCondition.getFunctionCall().getOperands().get(0);
    assertEquals(leftOperand.getFunctionCall().getOperator(), "decryptDouble");

    // Second condition should remain unchanged
    Expression rightCondition = filterExpr.getFunctionCall().getOperands().get(1);
    assertEquals(rightCondition.getFunctionCall().getOperator(), "EQUALS");
    Expression rightOperand = rightCondition.getFunctionCall().getOperands().get(0);
    assertEquals(rightOperand.getType(), ExpressionType.IDENTIFIER);
    assertEquals(rightOperand.getIdentifier().getName(), NORMAL_COLUMN);
  }

  @Test
  public void testRewriteGroupByClause() throws Exception {
    PinotQuery query = CalciteSqlParser.compileToPinotQueryWithoutRewrites(
        "SELECT COUNT(*) FROM " + EAR_ENABLED_TABLE + " GROUP BY " + ENCRYPTED_DOUBLE_COLUMN);

    PinotQuery rewrittenQuery = _decryptRewriter.rewrite(query);

    List<Expression> groupByList = rewrittenQuery.getGroupByList();
    assertEquals(groupByList.size(), 1);

    Expression groupByExpr = groupByList.get(0);
    assertEquals(groupByExpr.getFunctionCall().getOperator(), "decryptDouble");
    assertEquals(groupByExpr.getFunctionCall().getOperands().size(), 2);
  }

  @Test
  public void testRewriteOrderByClause() throws Exception {
    PinotQuery query = CalciteSqlParser.compileToPinotQueryWithoutRewrites(
        "SELECT NORMAL_COLUMN FROM " + EAR_ENABLED_TABLE + " ORDER BY " + ENCRYPTED_DOUBLE_COLUMN);

    PinotQuery rewrittenQuery = _decryptRewriter.rewrite(query);

    List<Expression> orderByList = rewrittenQuery.getOrderByList();
    assertEquals(orderByList.size(), 1);

    Expression orderByExpr = orderByList.get(0);
    assertEquals(orderByExpr.getFunctionCall().getOperator(), "asc");
    verifyDecryptDouble(orderByExpr.getFunctionCall().getOperands().get(0));
  }

  @Test
  public void testRewriteOrderByClauseWithMultipleColumns() throws Exception {
    PinotQuery query = CalciteSqlParser.compileToPinotQueryWithoutRewrites(
        "SELECT NORMAL_COLUMN FROM " + EAR_ENABLED_TABLE + " ORDER BY "
                + ENCRYPTED_DOUBLE_COLUMN + ", " + NORMAL_COLUMN);

    PinotQuery rewrittenQuery = _decryptRewriter.rewrite(query);

    List<Expression> orderByList = rewrittenQuery.getOrderByList();
    assertEquals(orderByList.size(), 2);

    // First column should be rewritten
    Expression firstOrderByExpr = orderByList.get(0);
    assertEquals(firstOrderByExpr.getFunctionCall().getOperator(), "asc");
    verifyDecryptDouble(firstOrderByExpr.getFunctionCall().getOperands().get(0));

    // Second column should remain unchanged
    Expression secondOrderByExpr = orderByList.get(1);
    assertEquals(secondOrderByExpr.getFunctionCall().getOperator(), "asc");
    assertEquals(secondOrderByExpr.getFunctionCall().getOperands().get(0).getIdentifier().getName(), NORMAL_COLUMN);
  }

  @Test
  public void testRewriteNestedFunctionExpression() throws Exception {
    PinotQuery query = CalciteSqlParser.compileToPinotQueryWithoutRewrites(
        "SELECT SUM(" + ENCRYPTED_DOUBLE_COLUMN + ") FROM " + EAR_ENABLED_TABLE);

    PinotQuery rewrittenQuery = _decryptRewriter.rewrite(query);

    Expression selectExpr = rewrittenQuery.getSelectList().get(0);
    assertEquals(selectExpr.getFunctionCall().getOperator(), "sum");

    // The operand of SUM should be the decrypt function with alias
    Expression sumOperand = selectExpr.getFunctionCall().getOperands().get(0);
    verifyDecryptDouble(sumOperand);
  }
  @Test
  public void testAlreadyAliasedEncryptedColumn() throws Exception {
    String aliasedName = "aliasedName";
    PinotQuery query = CalciteSqlParser.compileToPinotQueryWithoutRewrites(
        "SELECT " + ENCRYPTED_DOUBLE_COLUMN + " AS " + aliasedName + " FROM " + EAR_ENABLED_TABLE);

    PinotQuery rewrittenQuery = _decryptRewriter.rewrite(query);

    Expression selectExpr = rewrittenQuery.getSelectList().get(0);
    assertEquals(selectExpr.getFunctionCall().getOperator(), "as");
    Expression decryptExpr = selectExpr.getFunctionCall().getOperands().get(0);
    verifyDecryptDouble(decryptExpr);
    Expression aliasExpr = selectExpr.getFunctionCall().getOperands().get(1);
    assertEquals(aliasExpr.getIdentifier().getName(), aliasedName);
  }


  @Test
  public void testRewriteWithUnregisteredTable() throws Exception {
    // Don't register the table
    PinotQuery query = CalciteSqlParser.compileToPinotQueryWithoutRewrites(
        "SELECT " + ENCRYPTED_DOUBLE_COLUMN + " FROM " + UNREGISTERED_TABLE);

    PinotQuery rewrittenQuery = _decryptRewriter.rewrite(query);

    // Query should remain unchanged
    Expression selectExpr = rewrittenQuery.getSelectList().get(0);
    assertEquals(selectExpr.getType(), ExpressionType.IDENTIFIER);
    assertEquals(selectExpr.getIdentifier().getName(), ENCRYPTED_DOUBLE_COLUMN);
  }

  @Test
  public void testRewriteWithExistingDecryptFunction() throws Exception {
    // Create a query that already has a decrypt function
    PinotQuery query = new PinotQuery();
    DataSource dataSource = new DataSource();
    dataSource.setTableName(EAR_ENABLED_TABLE);
    query.setDataSource(dataSource);

    // Create a decrypt function expression
    Expression decryptExpr = createFunctionExpression("decryptDouble",
        Arrays.asList(createIdentifierExpression(KLU_COLUMN), createIdentifierExpression(ENCRYPTED_DOUBLE_COLUMN)));
    query.setSelectList(Arrays.asList(decryptExpr));

    PinotQuery rewrittenQuery = _decryptRewriter.rewrite(query);

    // Existing decrypt function should not be modified
    Expression selectExpr = rewrittenQuery.getSelectList().get(0);
    assertEquals(selectExpr.getFunctionCall().getOperator(), "decryptDouble");
  }

  @Test
  public void testRewriteWithJoinQuery() throws Exception {
    // Create a query with JOIN
    PinotQuery query = new PinotQuery();
    DataSource dataSource = new DataSource();
    dataSource.setTableName(EAR_ENABLED_TABLE);
    dataSource.setJoin(new Join());
    query.setDataSource(dataSource);
    query.setSelectList(Arrays.asList(createIdentifierExpression("*")));

    // JOIN queries should be returned unchanged (not supported yet)
    PinotQuery rewrittenQuery = _decryptRewriter.rewrite(query);

    // Query should remain unchanged - no rewriting should occur
    assertEquals(rewrittenQuery.getSelectList().size(), 1);
    Expression selectExpr = rewrittenQuery.getSelectList().get(0);
    assertEquals(selectExpr.getType(), ExpressionType.IDENTIFIER);
    assertEquals(selectExpr.getIdentifier().getName(), "*");
  }

  @Test
  public void testRewriteWithMultipleEncryptedColumns() throws Exception {
    Set<String> encryptedColumns = new HashSet<>();
    encryptedColumns.add(ENCRYPTED_DOUBLE_COLUMN);
    encryptedColumns.add(ENCRYPTED_LONG_COLUMN);

    PinotQuery query = CalciteSqlParser.compileToPinotQueryWithoutRewrites(
        "SELECT " + ENCRYPTED_DOUBLE_COLUMN + ", " + ENCRYPTED_LONG_COLUMN + " FROM " + EAR_ENABLED_TABLE);

    PinotQuery rewrittenQuery = _decryptRewriter.rewrite(query);

    // Both columns should be rewritten
    assertEquals(rewrittenQuery.getSelectList().size(), 2);

    Expression firstSelectExpr = rewrittenQuery.getSelectList().get(0);
    assertEquals(firstSelectExpr.getFunctionCall().getOperator(), "as");

    Expression secondSelectExpr = rewrittenQuery.getSelectList().get(1);
    assertEquals(secondSelectExpr.getFunctionCall().getOperator(), "as");
  }

  @Test
  public void testComplexQueryRewrite() throws Exception {
    PinotQuery query = CalciteSqlParser.compileToPinotQueryWithoutRewrites(
        "SELECT " + ENCRYPTED_DOUBLE_COLUMN + ", " + NORMAL_COLUMN + ", COUNT(*) "
        + "FROM " + EAR_ENABLED_TABLE + " "
        + "WHERE " + ENCRYPTED_DOUBLE_COLUMN + " > 100 AND " + NORMAL_COLUMN + " = 'test' "
        + "GROUP BY " + ENCRYPTED_DOUBLE_COLUMN + ", " + NORMAL_COLUMN + " "
        + "ORDER BY " + ENCRYPTED_DOUBLE_COLUMN + " DESC");

    PinotQuery rewrittenQuery = _decryptRewriter.rewrite(query);

    // Verify SELECT clause
    assertEquals(rewrittenQuery.getSelectList().size(), 3);
    Expression encryptedSelectExpr = rewrittenQuery.getSelectList().get(0);
    assertEquals(encryptedSelectExpr.getFunctionCall().getOperator(), "as");
    verifyDecryptDouble(encryptedSelectExpr.getFunctionCall().getOperands().get(0));

    // Verify WHERE clause contains decrypt function
    Expression filterExpr = rewrittenQuery.getFilterExpression();
    assertEquals(filterExpr.getFunctionCall().getOperator(), "AND");
    Expression leftCondition = filterExpr.getFunctionCall().getOperands().get(0);
    assertEquals(leftCondition.getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(), "decryptDouble");
    verifyDecryptDouble(leftCondition.getFunctionCall().getOperands().get(0));

    // Verify GROUP BY clause
    assertEquals(rewrittenQuery.getGroupByList().size(), 2);
    verifyDecryptDouble(rewrittenQuery.getGroupByList().get(0));

    // Verify ORDER BY clause
    assertEquals(rewrittenQuery.getOrderByList().size(), 1);
    assertEquals(rewrittenQuery.getOrderByList().get(0).getFunctionCall().getOperator(), "desc");
    verifyDecryptDouble(rewrittenQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0));
  }

  @Test
  public void testRewriteWithNullExpression() throws Exception {
    PinotQuery query = new PinotQuery();
    DataSource dataSource = new DataSource();
    dataSource.setTableName(EAR_ENABLED_TABLE);
    query.setDataSource(dataSource);
    query.setSelectList(Arrays.asList(createIdentifierExpression("*")));
    query.setFilterExpression(null); // Null filter expression

    PinotQuery rewrittenQuery = _decryptRewriter.rewrite(query);

    // Should handle null expressions gracefully
    assertNull(rewrittenQuery.getFilterExpression());
  }

  @Test
  public void testRewriteWithDisabledEar() throws Exception {
    PinotQuery query = CalciteSqlParser.compileToPinotQueryWithoutRewrites(
        "SELECT " + ENCRYPTED_DOUBLE_COLUMN + " FROM " + EAR_DISABLED_TABLE);

    PinotQuery rewrittenQuery = _decryptRewriter.rewrite(query);

    // Should not rewrite since EAR is disabled
    assertEquals(rewrittenQuery.getSelectList().size(), 1);
    assertEquals(rewrittenQuery.getSelectList().get(0).getIdentifier().getName(), ENCRYPTED_DOUBLE_COLUMN);
  }

  @Test
  public void testRegisterEarEnabledTable() throws Exception {
    final String tableName = "TestingRegistration";
    TableConfig mockTableConfig = mock(TableConfig.class);
    when(mockTableConfig.getTableName()).thenReturn(tableName);
    try (MockedConstruction<EarInfo> mockedEarInfo = mockConstruction(EarInfo.class, (mock, context) -> {
              when(mock.isEarEnabled()).thenReturn(true);
              when(mock.getEarKLUColumn()).thenReturn(KLU_COLUMN);
              when(mock.getEarEncryptedColumns()).thenReturn(ENCRYPTED_COLS_AND_TYPES);
    })) {
      _decryptRewriter.registerTable(mockTableConfig);
      _decryptRewriter.registerTable(mockTableConfig);
      _decryptRewriter.deregisterTable(mockTableConfig);
    }
  }

  @Test
  public void testRegisterEarDisabledTable() throws Exception {
    final String tableName = "TestingRegistrationOfDisabled";
    TableConfig mockTableConfig = mock(TableConfig.class);
    when(mockTableConfig.getTableName()).thenReturn(tableName);
    try (MockedConstruction<EarInfo> mockedEarInfo = mockConstruction(EarInfo.class, (mock, context) -> {
      when(mock.isEarEnabled()).thenReturn(false);
      when(mock.getEarKLUColumn()).thenReturn(null);
      when(mock.getEarEncryptedColumns()).thenReturn(null);
    })) {
      _decryptRewriter.registerTable(mockTableConfig);
      _decryptRewriter.registerTable(mockTableConfig);
      _decryptRewriter.deregisterTable(mockTableConfig);
    }
  }

  @Test
  public void testDeregisterTableWithNullTableConfig() {
    // Reproduces the broker ONLINE -> DROPPED path where the table config fetched from the property store is null
    // because the table is being dropped. Deregistration must be a no-op rather than throwing an NPE.
    _decryptRewriter.deregisterTable(null);
  }

  @Test
  public void testDeregisterTableWithNullTableName() {
    // A non-null config with a null table name must also be handled gracefully (extractRawTableName would return null
    // and ConcurrentHashMap.remove(null) would otherwise throw).
    TableConfig mockTableConfig = mock(TableConfig.class);
    when(mockTableConfig.getTableName()).thenReturn(null);
    _decryptRewriter.deregisterTable(mockTableConfig);
  }
}
