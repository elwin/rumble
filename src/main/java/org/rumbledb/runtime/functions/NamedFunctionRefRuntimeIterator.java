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

package org.rumbledb.runtime.functions;

import org.rumbledb.api.Item;
import org.rumbledb.context.FunctionIdentifier;
import org.rumbledb.exceptions.ExceptionMetadata;
import org.rumbledb.exceptions.IteratorFlowException;
import org.rumbledb.exceptions.UnknownFunctionCallException;
import org.rumbledb.items.FunctionItem;
import org.rumbledb.runtime.LocalRuntimeIterator;
import org.rumbledb.runtime.RuntimeIterator;

import sparksoniq.jsoniq.ExecutionMode;

public class NamedFunctionRefRuntimeIterator extends LocalRuntimeIterator {

    private static final long serialVersionUID = 1L;

    private FunctionIdentifier functionIdentifier;

    public NamedFunctionRefRuntimeIterator(
            FunctionIdentifier functionIdentifier,
            ExecutionMode executionMode,
            ExceptionMetadata iteratorMetadata
    ) {
        super(null, executionMode, iteratorMetadata);
        this.functionIdentifier = functionIdentifier;
    }

    @Override
    public Item next() {
        if (this.hasNext) {
            this.hasNext = false;
            if (
                !this.currentDynamicContextForLocalExecution.getNamedFunctions()
                    .checkUserDefinedFunctionExists(this.functionIdentifier)
            ) {
                throw new UnknownFunctionCallException(
                        this.functionIdentifier.getName(),
                        this.functionIdentifier.getArity(),
                        getMetadata()
                );
            }
            FunctionItem function = this.currentDynamicContextForLocalExecution.getNamedFunctions()
                .getUserDefinedFunction(this.functionIdentifier);
            FunctionItem result = ((FunctionItem) function).deepCopy();
            result.populateClosureFromDynamicContext(this.currentDynamicContextForLocalExecution, getMetadata());
            return result;
        }

        throw new IteratorFlowException(
                RuntimeIterator.FLOW_EXCEPTION_MESSAGE + this.functionIdentifier,
                getMetadata()
        );
    }
}