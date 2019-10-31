/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package sparksoniq.jsoniq.item;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.rumbledb.api.Item;
import sparksoniq.exceptions.FunctionsNonSerializableException;
import sparksoniq.exceptions.SparksoniqRuntimeException;
import sparksoniq.jsoniq.runtime.iterator.RuntimeIterator;
import sparksoniq.jsoniq.runtime.iterator.functions.base.FunctionIdentifier;
import sparksoniq.semantics.types.ItemType;
import sparksoniq.semantics.types.ItemTypes;
import sparksoniq.semantics.types.SequenceType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FunctionItem extends Item {

    private static final long serialVersionUID = 1L;
    private FunctionIdentifier identifier;
    private List<String> parameterNames;

    // signature contains type information for all parameters and the return value
    private List<SequenceType> signature;

    private RuntimeIterator bodyIterator;
    private Map<String, Item> nonLocalVariableBindings;

    protected FunctionItem() {
        super();
    }

    public FunctionItem(FunctionIdentifier identifier, List<String> parameterNames, List<SequenceType> signature, RuntimeIterator bodyIterator, Map<String, Item> nonLocalVariableBindings) {
        this.identifier = identifier;
        this.parameterNames = parameterNames;
        this.signature = signature;
        this.bodyIterator = bodyIterator;
        this.nonLocalVariableBindings = nonLocalVariableBindings;
    }

    public FunctionItem(String name, Map<String, SequenceType> paramNameToSequenceTypes, SequenceType returnType, RuntimeIterator bodyIterator) {
        List<String> paramNames = new ArrayList<>();
        List<SequenceType> signature = new ArrayList<>();
        for (Map.Entry<String, SequenceType> paramEntry : paramNameToSequenceTypes.entrySet()) {
            paramNames.add(paramEntry.getKey());
            signature.add(paramEntry.getValue());
        }
        signature.add(returnType);

        this.identifier = new FunctionIdentifier(name, paramNames.size());
        this.parameterNames = paramNames;
        this.signature = signature;
        this.bodyIterator = bodyIterator;
        this.nonLocalVariableBindings = new HashMap<>();
    }

    public FunctionIdentifier getIdentifier() {
        return identifier;
    }

    public List<String> getParameterNames() {
        return parameterNames;
    }

    public List<SequenceType> getSignature() {
        return signature;
    }

    public RuntimeIterator getBodyIterator() {
        return bodyIterator;
    }

    public Map<String, Item> getNonLocalVariableBindings() {
        return nonLocalVariableBindings;
    }

    @Override
    public boolean equals(Object other) {
        // functions can not be compared
        return false;
    }

    @Override
    public boolean getEffectiveBooleanValue() {
        return false;
    }

    @Override
    public boolean isAtomic() {
        return false;
    }

    @Override
    public boolean isTypeOf(ItemType type) {
        return type.getType().equals(ItemTypes.FunctionItem) || type.getType().equals(ItemTypes.Item);
    }

    @Override
    public String serialize() {
        throw new FunctionsNonSerializableException();
    }

    @Override
    public int hashCode() {
        return this.identifier.hashCode() + String.join("", this.parameterNames).hashCode();
    }

    @Override
    public void write(Kryo kryo, Output output) {
        kryo.writeObject(output, this.identifier);
        kryo.writeObject(output, this.parameterNames);
        kryo.writeObject(output, this.signature);
        // kryo.writeObject(output, this.bodyIterator);
        kryo.writeObject(output, this.nonLocalVariableBindings);

        // convert RuntimeIterator to byte[] data
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(this.bodyIterator);
            oos.flush();
            byte [] data = bos.toByteArray();
            output.writeInt(data.length);
            output.writeBytes(data);
        } catch (Exception e) {
            throw new SparksoniqRuntimeException(
                    "Error converting functionItem-bodyRuntimeIterator to byte[]:" + e.getMessage()
            );
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void read(Kryo kryo, Input input) {
        this.identifier = kryo.readObject(input, FunctionIdentifier.class);
        this.parameterNames = kryo.readObject(input, ArrayList.class);
        this.signature = kryo.readObject(input, ArrayList.class);
        // this.bodyIterator = kryo.readObject(input, RuntimeIterator.class);
        this.nonLocalVariableBindings = kryo.readObject(input, HashMap.class);

        try {
            int dataLength = input.readInt();
            byte[] data = input.readBytes(dataLength);
            ByteArrayInputStream bis = new ByteArrayInputStream(data);
            ObjectInputStream ois = new ObjectInputStream(bis);
            this.bodyIterator= (RuntimeIterator) ois.readObject();
        } catch (Exception e) {
            throw new SparksoniqRuntimeException(
                    "Error converting functionItem-bodyRuntimeIterator to functionItem:" + e.getMessage()
            );
        }
    }
}
