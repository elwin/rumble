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
 * Authors: Stefan Irimescu, Can Berker Cikis, Ghislain Fourny
 *
 */

package org.rumbledb.runtime.flwor.udfs;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.StructType;
import org.rumbledb.api.Item;
import org.rumbledb.context.DynamicContext;
import org.rumbledb.context.Name;
import org.rumbledb.exceptions.OurBadException;
import org.rumbledb.expressions.flowr.OrderByClauseSortingKey;
import org.rumbledb.items.NullItem;
import org.rumbledb.runtime.RuntimeIterator;
import org.rumbledb.runtime.flwor.expression.OrderByClauseAnnotatedChildIterator;
import org.rumbledb.types.BuiltinTypesCatalogue;
import org.rumbledb.types.ItemType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BinaryOrderClauseCreateColumnsUDF implements UDF1<Row, Row> {

    private static final long serialVersionUID = 1L;
    private DataFrameContext dataFrameContext;
    private List<OrderByClauseAnnotatedChildIterator> expressionsWithIterator;

    private Map<Integer, Name> sortingKeyTypes;

    private List<Object> results;

    // nulls and empty sequences have special ordering captured in the first sorting column
    // if non-null, non-empty-sequence value is given, the second column is used to sort the input
    // indices are assigned to each value type for the first column
    private static int emptySequenceOrderIndexFirst = 1; // by default, empty sequence is taken as first(=least)
    private static int emptySequenceOrderIndexLast = 4; // by default, empty sequence is taken as first(=least)
    private static int nullOrderIndex = 2; // null is the smallest value except empty sequence(default)
    private static int valueOrderIndex = 3; // values are larger than null and empty sequence(default)

    private static final List<Name> types = Stream.of(
        BuiltinTypesCatalogue.booleanItem,
        BuiltinTypesCatalogue.stringItem,
        BuiltinTypesCatalogue.integerItem,
        BuiltinTypesCatalogue.doubleItem,
        BuiltinTypesCatalogue.floatItem,
        BuiltinTypesCatalogue.decimalItem,
        BuiltinTypesCatalogue.durationItem,
        BuiltinTypesCatalogue.yearMonthDurationItem,
        BuiltinTypesCatalogue.dayTimeDurationItem,
        BuiltinTypesCatalogue.dateTimeItem,
        BuiltinTypesCatalogue.dateItem,
        BuiltinTypesCatalogue.timeItem
    ).map(ItemType::getName).collect(Collectors.toList());

    public BinaryOrderClauseCreateColumnsUDF(
            List<OrderByClauseAnnotatedChildIterator> expressionsWithIterator,
            DynamicContext context,
            StructType schema,
            Map<Integer, Name> sortingKeyTypes,
            List<String> columnNames
    ) {
        this.dataFrameContext = new DataFrameContext(context, schema, columnNames);
        this.expressionsWithIterator = expressionsWithIterator;
        this.sortingKeyTypes = sortingKeyTypes;

        this.results = new ArrayList<>();
    }

    @Override
    public Row call(Row row) {
        this.dataFrameContext.setFromRow(row);

        this.results.clear();

        for (int expressionIndex = 0; expressionIndex < this.expressionsWithIterator.size(); expressionIndex++) {
            OrderByClauseAnnotatedChildIterator expressionWithIterator = this.expressionsWithIterator.get(
                expressionIndex
            );

            // apply expression in the dynamic context
            RuntimeIterator iterator = expressionWithIterator.getIterator();
            iterator.open(this.dataFrameContext.getContext());
            if (!iterator.hasNext()) {
                if (expressionWithIterator.getEmptyOrder() == OrderByClauseSortingKey.EMPTY_ORDER.GREATEST) {
                    this.results.add(emptySequenceOrderIndexLast);
                } else {
                    this.results.add(emptySequenceOrderIndexFirst);
                }
                this.results.add(null); // placeholder for valueColumn(2nd column)
                iterator.close();
                continue;
            }
            while (iterator.hasNext()) {
                Item nextItem = iterator.next();
                createColumnsForItem(nextItem, expressionIndex);
            }
            iterator.close();

        }
        return RowFactory.create(this.results.toArray());
    }


    private void createColumnsForItem(Item nextItem, int expressionIndex) {
        if (nextItem instanceof NullItem) {
            this.results.add(nullOrderIndex);
            this.results.add(null); // placeholder for valueColumn(2nd column)
        } else {
            // any other atomic type
            this.results.add(valueOrderIndex);

            if (false) {
                // extract type information for the sorting column
                Name typeName = this.sortingKeyTypes.get(expressionIndex);
                if (!types.contains(typeName)) {
                    throw new OurBadException(
                            "Unexpected ordering type found while creating columns."
                    );
                }
            }

            this.results.add(nextItem.getBinaryKey());
        }
    }
}
