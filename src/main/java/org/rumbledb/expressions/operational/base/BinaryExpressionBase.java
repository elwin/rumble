/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Authors: Stefan Irimescu, Can Berker Cikis
 *
 */

package org.rumbledb.expressions.operational.base;


import org.rumbledb.exceptions.ExceptionMetadata;
import org.rumbledb.expressions.Expression;
import org.rumbledb.expressions.Node;

import java.util.ArrayList;
import java.util.List;

public abstract class BinaryExpressionBase extends OperationalExpressionBase {

    private Expression rightExpression;

    public BinaryExpressionBase(
            Expression mainExpression,
            Expression rhs,
            Operator op,
            ExceptionMetadata metadata
    ) {
        super(mainExpression, op, metadata);
        this.rightExpression = rhs;
    }

    public Expression getRightExpression() {
        return this.rightExpression;
    }

    public Operator getOperator() {
        return this.singleOperator;
    }

    @Override
    public List<Node> getChildren() {
        List<Node> result = new ArrayList<>();
        result.add(this.mainExpression);
        if (this.rightExpression != null) {
            result.add(this.rightExpression);
        }
        return result;
    }
}
