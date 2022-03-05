package aggregations.builtins;

import aggregations.Aggregation;
import streamingtuples.builtins.DoubleTuple;


public class MaxDouble implements Aggregation<DoubleTuple, DoubleTuple, DoubleTuple> {

    @Override
    public DoubleTuple createAccumulator() {
        return DoubleTuple.MIN_VALUE;
    }

    @Override
    public DoubleTuple merge(DoubleTuple write, DoubleTuple read) {
        return (write.getValue() >= read.getValue())
                ? write
                : read;
    }

    @Override
    public DoubleTuple getResult(DoubleTuple partialAggregation) {
        return partialAggregation;
    }

    @Override
    public DoubleTuple add(DoubleTuple partialAggregation, DoubleTuple input) {
        return (partialAggregation.getValue() >= input.getValue())
                ? partialAggregation
                : input;
    }

    @Override
    public void computeLeftCumulativeSliceAggregation(DoubleTuple[] array) {
        for (int i = array.length - 2; i > 0; i--)
            array[i] = (array[i].getValue() >= array[i + 1].getValue()) ? array[i] : array[i + 1];
    }

    @Override
    public String toString() {
        return "Max";
    }
}
