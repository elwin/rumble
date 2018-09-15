package sparksoniq.jsoniq.runtime.iterator.functions.arrays;

import sparksoniq.exceptions.IteratorFlowException;
import sparksoniq.jsoniq.item.Item;
import sparksoniq.jsoniq.runtime.iterator.RuntimeIterator;
import sparksoniq.jsoniq.runtime.metadata.IteratorMetadata;

import javax.naming.OperationNotSupportedException;
import java.util.ArrayList;
import java.util.List;

public class ArrayDescendantFunctionIterator extends ArrayFunctionIterator {
    public ArrayDescendantFunctionIterator(List<RuntimeIterator> arguments, IteratorMetadata iteratorMetadata) {
        super(arguments, ArrayFunctionOperators.DESCENDANT, iteratorMetadata);
    }

    @Override
    public Item next() {
        if (this._hasNext) {
            if (results == null) {
                _currentIndex = 0;
                results = new ArrayList<>();
                RuntimeIterator sequenceIterator = this._children.get(0);
                List<Item> items = getItemsFromIteratorWithCurrentContext(sequenceIterator);
                getDescendantArrays(items);
            }
            return getResult();
        }
        throw new IteratorFlowException(RuntimeIterator.FLOW_EXCEPTION_MESSAGE + " DESCENDANT-ARRAYS function",
                getMetadata());
    }

    public void getDescendantArrays(List<Item> items) {
        for (Item item:items) {
            if (item.isArray()) {
                results.add(item);
                try {
                    getDescendantArrays(item.getItems());
                } catch (OperationNotSupportedException e) {
                    e.printStackTrace();
                }
            }
            else if (item.isObject()) {
                try {
                    getDescendantArrays((List<Item>) item.getValues());
                } catch (OperationNotSupportedException e) {
                    e.printStackTrace();
                }
            }
            else {
                // do nothing
            }
        }
    }
}