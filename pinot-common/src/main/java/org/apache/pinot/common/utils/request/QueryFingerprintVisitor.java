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
package org.apache.pinot.common.utils.request;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;


/**
 * QueryFingerprintVisitor traverses the Calcite SqlNode AST and produces a normalized query fingerprint.
 * Implementation is based on Calcite 1.40.0 version. It may change in future versions of Calcite.
 *
 * <p>
 * <ul>
 *   <li>All data literals are replaced with dynamic parameters (?)</li>
 *   <li>IN/NOT IN clauses with an explicit value list are squashed to a single parameter, regardless of
 *       what the list contains (literals, expressions, function calls, NULL). Subqueries are visited normally.</li>
 *   <li>EXPLAIN PLAN FOR is NOT preserved.</li>
 *   <li>Symbolic keywords (DISTINCT, ASC, DESC, etc.) and NULL literals are preserved, except that a NULL
 *       inside an explicit IN/NOT IN value list is squashed along with the rest of the list (see above).</li>
 *   <li>Hints are preserved.</li>
 *   <li>Window functions: window specification (operand 1) are preserved.</li>
 * </ul>
 * </p>
 *
 * <p><b>Important:</b> This visitor is non-mutating. It returns new SqlNode trees and never modifies
 * the original nodes via setOperand(). This is critical because the original SqlNode in SqlNodeAndOptions
 * must remain intact for subsequent compilation (e.g., compileToPinotQuery).</p>
 *
 * <p><b>Note:</b> This visitor maintains internal state (dynamic parameter index) that is not reset between visits.
 * A new instance should be created for each query fingerprint.</p>
 */
public class QueryFingerprintVisitor extends SqlShuttle {
  // SqlSelect operand indices, see {@link org.apache.calcite.sql.SqlSelect}.
  // Calcite 1.40 SqlSelect.getOperandList():
  //   [0] keywordList, [1] selectList, [2] from, [3] where, [4] groupBy,
  //   [5] having, [6] windowDecls, [7] qualify, [8] orderList, [9] offset, [10] fetch, [11] hints
  private static final int SQLSELECT_HINTS_OPERAND_INDEX = 11;

  // SqlJoin operand indices, see {@link org.apache.calcite.sql.SqlJoin}.
  // SqlJoin.getOperandList(): [0] left, [1] natural, [2] joinType, [3] right, [4] conditionType, [5] condition
  private static final int SQLJOIN_LEFT_INDEX = 0;
  private static final int SQLJOIN_NATURAL_INDEX = 1;
  private static final int SQLJOIN_JOIN_TYPE_INDEX = 2;
  private static final int SQLJOIN_RIGHT_INDEX = 3;
  private static final int SQLJOIN_CONDITION_TYPE_INDEX = 4;
  private static final int SQLJOIN_CONDITION_INDEX = 5;

  private int _dynamicParamIndex;

  public QueryFingerprintVisitor() {
    _dynamicParamIndex = 0;
  }

  @Override
  public SqlNode visit(SqlLiteral literal) {
    if (shouldPreserveLiteral(literal)) {
      return literal;
    }
    // Replace data literals (numbers, strings, dates, etc.) with dynamic parameters
    return new SqlDynamicParam(_dynamicParamIndex++, literal.getParserPosition());
  }

  @Override
  public @Nullable SqlNode visit(SqlCall call) {
    SqlNode result = call;
    switch (call.getKind()) {
      case SELECT:
        result = visitSelect((SqlSelect) call);
        break;
      case JOIN:
        result = visitJoin((SqlJoin) call);
        break;
      case WITH:
        result = visitWith((SqlWith) call);
        break;
      case WITH_ITEM:
        result = visitWithItem((SqlWithItem) call);
        break;
      // EXPLAIN PLAN FOR is NOT preserved.
      case EXPLAIN:
        result = call.getOperandList().get(0).accept(this);
        break;
      case CASE:
        result = visitCase((SqlCase) call);
        break;
      case ORDER_BY:
        result = visitOrderBy((SqlOrderBy) call);
        break;
      case OVER:
        // Window functions: only visit the aggregate function (operand 0).
        // Skip the window specification (operand 1) due to its complex structure
        // with ORDER BY and frame clauses. This means literals in PARTITION BY,
        // ORDER BY, and window frames are preserved rather than replaced.
        result = createCallWithReplacedOperand(call, 0, call.getOperandList().get(0).accept(this));
        break;
      case IN:
      case NOT_IN:
        result = visitIn(call);
        break;
      default:
        return super.visit(call);
    }
    return result;
  }

  @Nullable
  private SqlNode visitCase(SqlCase sqlCase) {
    // Use SqlCase's typed accessors so each operand is visited with the matching
    // helper (visitNodeList for SqlNodeList, visitIfPresent for SqlNode), avoiding
    // unchecked casts on a generic operand list.
    return new SqlCase(
        sqlCase.getParserPosition(),
        visitIfPresent(sqlCase.getValueOperand()),
        visitNodeList(sqlCase.getWhenOperands()),
        visitNodeList(sqlCase.getThenOperands()),
        visitIfPresent(sqlCase.getElseOperand()));
  }

  @Nullable
  private SqlNode visitSelect(SqlSelect select) {
    // Access operands by index to avoid package-private field access.
    // See SQLSELECT operand index reference in the class header.
    List<SqlNode> ops = select.getOperandList();
    return new SqlSelect(
        select.getParserPosition(),
        visitNodeList((SqlNodeList) ops.get(0)),   // keywordList
        visitNodeList((SqlNodeList) ops.get(1)),   // selectList
        visitIfPresent(ops.get(2)),                 // from
        visitIfPresent(ops.get(3)),                 // where
        visitNodeList((SqlNodeList) ops.get(4)),   // groupBy
        visitIfPresent(ops.get(5)),                 // having
        visitNodeList((SqlNodeList) ops.get(6)),   // windowDecls
        visitIfPresent(ops.get(7)),                 // qualify
        visitNodeList((SqlNodeList) ops.get(8)),   // orderList
        visitIfPresent(ops.get(9)),                 // offset
        visitIfPresent(ops.get(10)),                // fetch
        (SqlNodeList) ops.get(SQLSELECT_HINTS_OPERAND_INDEX)  // hints (preserved)
    );
  }

  @Nullable
  private SqlNode visitJoin(SqlJoin join) {
    List<SqlNode> operands = join.getOperandList();
    // Visit data operands (left, right, condition) but preserve metadata literals
    // (natural, joinType, conditionType) which are structural keywords, not data literals.
    SqlNode newLeft = operands.get(SQLJOIN_LEFT_INDEX).accept(this);
    SqlNode newRight = operands.get(SQLJOIN_RIGHT_INDEX).accept(this);
    SqlNode condition = operands.get(SQLJOIN_CONDITION_INDEX);
    SqlNode newCondition = condition != null ? condition.accept(this) : null;

    return new SqlJoin(
        join.getParserPosition(),
        newLeft,
        (SqlLiteral) operands.get(SQLJOIN_NATURAL_INDEX),
        (SqlLiteral) operands.get(SQLJOIN_JOIN_TYPE_INDEX),
        newRight,
        (SqlLiteral) operands.get(SQLJOIN_CONDITION_TYPE_INDEX),
        newCondition);
  }

  @Nullable
  private SqlNode visitWith(SqlWith with) {
    List<SqlNode> newList = new ArrayList<>();
    for (SqlNode node : with.withList.getList()) {
      newList.add(node.accept(this));
    }
    SqlNode newBody = with.body.accept(this);
    // Use SqlWithOperator.createCall() to construct a new SqlWith
    return with.getOperator().createCall(
        with.getFunctionQuantifier(),
        with.getParserPosition(),
        new SqlNodeList(newList, with.withList.getParserPosition()),
        newBody);
  }

  /**
   * SqlWithItem has four fields: SqlIdentifier name, SqlNodeList
   * columnList, SqlNode query, and SqlLiteral recursive.
   * We will visit only the columnList and query since:
   * - name has already been visited in the SqlWith visit method.
   * - recursive is a literal which is a property of the WITH item and not the query itself.
   */
  @Nullable
  private SqlNode visitWithItem(SqlWithItem withItem) {
    List<SqlNode> operands = withItem.getOperandList();
    SqlNodeList columnList = withItem.columnList;
    if (columnList != null) {
      List<SqlNode> newColumns = new ArrayList<>(columnList.size());
      for (SqlNode column : columnList) {
        newColumns.add(column != null ? column.accept(this) : null);
      }
      columnList = new SqlNodeList(newColumns, columnList.getParserPosition());
    }
    SqlNode newQuery = withItem.query != null ? withItem.query.accept(this) : null;

    // SqlWithItem operands: [name, columnList, query, recursive]
    return withItem.getOperator().createCall(
        withItem.getFunctionQuantifier(),
        withItem.getParserPosition(),
        operands.get(0),      // name (preserved)
        columnList,
        newQuery,
        operands.get(3));     // recursive (preserved)
  }

  @Nullable
  private SqlNode visitOrderBy(SqlOrderBy orderBy) {
    return new SqlOrderBy(
        orderBy.getParserPosition(),
        orderBy.query.accept(this),
        orderBy.orderList,
        orderBy.offset != null ? orderBy.offset.accept(this) : null,
        orderBy.fetch != null ? orderBy.fetch.accept(this) : null);
  }

  /**
   * Visit IN/NOT IN clause.
   * <p>
   * Two cases:
   * <ul>
   *   <li><b>Explicit value list</b> (second operand is a SqlNodeList) → squash the entire list to a single ?
   *       without inspecting its elements. This holds regardless of what the list contains:
   *       <ul>
   *         <li>Literals: IN (1, 2, 3) → IN (?)</li>
   *         <li>Expressions: IN (col1 + 1, 2) → IN (?)</li>
   *         <li>Function calls: IN (UPPER('a'), LOWER('b')) → IN (?)</li>
   *         <li>NULL: IN (1, NULL, 3) → IN (?)</li>
   *       </ul>
   *       Skipping the per-element traversal is the point of this path: large machine-generated IN lists
   *       (thousands of ids) collapse to a single parameter in O(1) instead of O(list size).
   *   </li>
   *   <li><b>Subquery</b> (second operand is a SqlSelect) → visit the subquery normally.
   *       <br>Example: IN (SELECT ...) → IN (SELECT ... with literals replaced)</li>
   * </ul>
   * </p>
   */
  @Nullable
  private SqlNode visitIn(SqlCall inCall) {
    List<SqlNode> operands = inCall.getOperandList();
    if (operands.isEmpty()) {
      return inCall;
    }

    // First operand is the column/expression being checked
    SqlNode leftOperand = operands.get(0).accept(this);

    // Second operand can be:
    // - SqlNodeList: an explicit value list, e.g. IN (1, 2, 3), IN (col + 1, UPPER('a'), NULL)
    // - SqlSelect: a subquery, e.g. IN (SELECT ...)
    if (operands.size() > 1 && operands.get(1) instanceof SqlNodeList) {
      SqlNodeList valueList = (SqlNodeList) operands.get(1);

      // Squash the entire value list to a single dynamic parameter without iterating its elements.
      SqlNodeList newValueList;
      if (valueList.size() > 0) {
        SqlDynamicParam singleParam = new SqlDynamicParam(_dynamicParamIndex++, inCall.getParserPosition());
        newValueList = new SqlNodeList(List.of(singleParam), valueList.getParserPosition());
      } else {
        newValueList = valueList;
      }

      // Create a new IN/NOT IN call with visited operands
      return inCall.getOperator().createCall(
          inCall.getParserPosition(),
          leftOperand,
          newValueList);
    }

    // Fallback: for subqueries or other non-SqlNodeList cases, visit the remaining operands normally.
    // Reuse the already-visited leftOperand (operand 0) instead of visiting it a second time.
    List<SqlNode> newOperands = new ArrayList<>(operands.size());
    newOperands.add(leftOperand);
    for (int i = 1; i < operands.size(); i++) {
      newOperands.add(operands.get(i).accept(this));
    }
    return inCall.getOperator().createCall(
        inCall.getParserPosition(),
        newOperands.toArray(new SqlNode[0]));
  }

  /**
   * Creates a new SqlCall with one operand replaced, preserving all other operands.
   * Used for cases like OVER where we only want to visit operand 0.
   */
  private static SqlCall createCallWithReplacedOperand(SqlCall call, int index, SqlNode newOperand) {
    List<SqlNode> operands = call.getOperandList();
    SqlNode[] newOperands = operands.toArray(new SqlNode[0]);
    newOperands[index] = newOperand;
    return (SqlCall) call.getOperator().createCall(
        call.getFunctionQuantifier(),
        call.getParserPosition(),
        newOperands);
  }

  @Nullable
  private SqlNode visitIfPresent(@Nullable SqlNode node) {
    return node != null ? node.accept(this) : null;
  }

  @Nullable
  private SqlNodeList visitNodeList(@Nullable SqlNodeList nodeList) {
    if (nodeList == null) {
      return null;
    }
    List<SqlNode> newNodes = new ArrayList<>(nodeList.size());
    for (SqlNode node : nodeList) {
      newNodes.add(node != null ? node.accept(this) : null);
    }
    return new SqlNodeList(newNodes, nodeList.getParserPosition());
  }

  /**
   * Check if a literal should be preserved.
   * Currently, we preserve symbolic keywords (DISTINCT, ASC, DESC, etc.) and NULL literals.
   */
  private boolean shouldPreserveLiteral(SqlLiteral literal) {
    return literal.getTypeName() == SqlTypeName.SYMBOL || literal.getTypeName() == SqlTypeName.NULL;
  }
}
