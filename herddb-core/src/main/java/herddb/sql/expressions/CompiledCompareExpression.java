/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */
package herddb.sql.expressions;

import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.sql.SQLRecordPredicate;
import java.util.Map;

public class CompiledCompareExpression implements CompiledSQLExpression {

    private final CompiledSQLExpression left;
    private final CompiledSQLExpression right;
    private final boolean not;
    private final Type type;
    
    public enum Type {
        EQUALS,
        NOT_EQUALS,
        GREATER_THEN,
        GREATER_THEN_EQUALS,
        MINOR_THEN,
        MINOR_THEN_EQUALS
    }

    public CompiledCompareExpression(Boolean not, CompiledSQLExpression left, CompiledSQLExpression right, Type type) {
        this.left = left;
        this.right = right;
        this.not = not;
        this.type = type;
    }

    @Override
    public Object evaluate(Map<String, Object> bean, StatementEvaluationContext context) throws StatementExecutionException {
        Object leftValue = left.evaluate(bean, context);
        Object rightValue = right.evaluate(bean, context);
        boolean res;
        
        switch (type) {
            case EQUALS:
                res = SQLRecordPredicate.objectEquals(leftValue, rightValue);
                break;
            case NOT_EQUALS:
                res = ! SQLRecordPredicate.objectEquals(leftValue, rightValue);
                break;
            case GREATER_THEN:
                res = SQLRecordPredicate.compare(leftValue, rightValue) > 0;
                break;
            case GREATER_THEN_EQUALS:
                res = SQLRecordPredicate.compare(leftValue, rightValue) >= 0;
                break;
            case MINOR_THEN:
                res = SQLRecordPredicate.compare(leftValue, rightValue) < 0;
                break;
            case MINOR_THEN_EQUALS:
                res = SQLRecordPredicate.compare(leftValue, rightValue) <= 0;
                break;
            default:
                throw new IllegalArgumentException(""+type);
        }
        
        if (not) {
            return !res;
        } else {
            return res;
        }
    }

}
