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

package org.rumbledb.items;

import org.rumbledb.api.Item;
import org.rumbledb.exceptions.ExceptionMetadata;
import org.rumbledb.runtime.typing.CastIterator;
import org.rumbledb.types.ItemType;

public abstract class AtomicItem extends ItemImpl {

    private static final long serialVersionUID = 1L;

    protected AtomicItem() {
        super();
    }

    @Override
    public boolean isAtomic() {
        return true;
    }

    @Override
    public boolean isTypeOf(ItemType type) {
        return type.equals(ItemType.atomicItem) || type.equals(ItemType.item);
    }

    @Override
    public Item promoteTo(ItemType type) {
        return CastIterator.castItemToType(this, type, ExceptionMetadata.EMPTY_METADATA);
    }
}
