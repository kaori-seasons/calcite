/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.dialect;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlFloorFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class DorisSqlDialect extends SqlDialect {

  public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(DatabaseProduct.DORIS)
      .withNullCollation(NullCollation.LOW);

  public static final SqlDialect DEFAULT = new DorisSqlDialect(DEFAULT_CONTEXT);

  /**
   * 设置需要解析的udf列表 没有就在udf当中定义
   */
  private static final SqlFunction DORISSQL_SUBSTRING =
      SqlBasicFunction.create("SUBSTRING", ReturnTypes.ARG0_NULLABLE_VARYING,
          OperandTypes.VARIADIC, SqlFunctionCategory.STRING);

  public DorisSqlDialect(Context context) {
    super(context);
  }

  @Override protected boolean allowsAs() {
    return false;
  }

  @Override public boolean supportsCharSet() {
    return false;
  }

  @Override public JoinType emulateJoinTypeForCrossJoin() {
    return JoinType.CROSS;
  }

  @Override public boolean supportsGroupByWithRollup() {
    return true;
  }

  @Override public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {

    if (call.getOperator() == SqlStdOperatorTable.SUBSTRING) {
      SqlUtil.unparseFunctionSyntax(DORISSQL_SUBSTRING, writer, call, false);
    } else {
      switch (call.getKind()) {
        case FLOOR:
          if (call.operandCount() != 2 ) {
            super.unparseCall(writer, call, leftPrec, rightPrec);
            return;
          }

          final SqlLiteral timeUnitNode = call.operand(1);
          final TimeUnitRange timeUnit = timeUnitNode.getValueAs(TimeUnitRange.class);

          SqlCall call2 = SqlFloorFunction.replaceTimeUnitOperand(call, timeUnit.name(), timeUnitNode.getParserPosition());
          SqlFloorFunction.unparseDatetimeFunction(writer, call2, "DATE_TRUNC", false);
          break;
      case AS:
        break;
      }
    }
  }
}
