/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Author: Stefan Irimescu
 *
 */
 package sparksoniq.jsoniq.runtime.iterator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import sparksoniq.exceptions.IteratorFlowException;
import sparksoniq.jsoniq.item.Item;
import sparksoniq.semantics.DynamicContext;
import org.apache.spark.api.java.JavaRDD;
import java.util.ArrayList;
import java.util.List;

public abstract class RuntimeIterator implements RuntimeIteratorInterface, KryoSerializable {
    protected static final String FLOW_EXCEPTION_MESSAGE = "Invalid next() call; ";
    public void open(DynamicContext context){
        if(this._isOpen)
            throw new IteratorFlowException("Runtime iterator cannot be opened twice");
        this._isOpen = true;
        this._hasNext = true;
        this._currentDynamicContext = context;
    }

    public void close(){
        this._isOpen = false;
        this._children.forEach(c -> c.close());
    }

    public void reset(DynamicContext context){
        this._hasNext = true;
        this._currentDynamicContext = context;
        this._children.forEach(c -> c.reset(context));
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeBoolean(_hasNext);
        output.writeBoolean(_isOpen);
        kryo.writeObject(output, this._currentDynamicContext);
        kryo.writeObject(output, this._children);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        this._hasNext = input.readBoolean();
        this._isOpen = input.readBoolean();
        this._currentDynamicContext = kryo.readObject(input, DynamicContext.class);
        this._children = kryo.readObject(input, ArrayList.class);
    }

    public boolean hasNext(){
        return this._hasNext;
    }

    public boolean isOpen() { return _isOpen;}

    public abstract boolean isRDD();

    public abstract JavaRDD<Item> getRDD();

    public abstract Item next();

    protected RuntimeIterator(List<RuntimeIterator> children){
        this._isOpen = false;
        this._children = new ArrayList<>();
        if(children!=null && !children.isEmpty())
            this._children.addAll(children);
    }

    protected boolean _hasNext;
    protected boolean _isOpen;
    protected List<RuntimeIterator> _children;
    protected DynamicContext _currentDynamicContext;

    protected List<Item> runChildrenIterators(DynamicContext context){
        List<Item> values = new ArrayList<>();

        for(RuntimeIterator iterator:this._children){
            iterator.open(context);
            while (iterator.hasNext())
                values.add(iterator.next());
            iterator.close();
        }
        return values;
    }



}