package aggregations.builtins;

import aggregations.Aggregation;
import streamingtuples.builtins.DoubleTuple;


public class MaxDouble2 implements Aggregation<DoubleTuple, DoubleTuple, DoubleTuple> {
    @Override
    public DoubleTuple createAccumulator() { // creates a new slice aggregation instance
        return new DoubleTuple(Double.MIN_VALUE);
    }

    @Override
    public DoubleTuple merge(DoubleTuple write, DoubleTuple read) {
        write.setValue(Math.max(write.getValue(), read.getValue()));
        return write;
    }

    @Override
    public DoubleTuple getResult(DoubleTuple partialAggregation) {
        return partialAggregation;
    }

    @Override
    public DoubleTuple add(DoubleTuple partialAggregation, DoubleTuple input) {
        partialAggregation.setValue(Math.max(partialAggregation.getValue(), input.getValue()));
        return partialAggregation;
    }

    @Override
    public void computeLeftCumulativeSliceAggregation(DoubleTuple[] array) {
        for (int i = array.length - 2; i > 0; i--)
            array[i].setValue(Math.max(array[i].getValue(), array[i + 1].getValue()));
    }

    @Override
    public String toString() {
        return "Max";
    }
}
