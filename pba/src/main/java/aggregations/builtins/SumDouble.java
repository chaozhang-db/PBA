package aggregations.builtins;

import aggregations.Aggregation;
import streamingtuples.builtins.DoubleTuple;

public class SumDouble implements Aggregation<DoubleTuple, DoubleTuple, DoubleTuple> {
    @Override
    public DoubleTuple createAccumulator() {
        return new DoubleTuple(0.0);
    }

    @Override
    public DoubleTuple merge(DoubleTuple write, DoubleTuple read) {
        write.setValue(write.getValue() + read.getValue());
        return write;
    }

    @Override
    public DoubleTuple getResult(DoubleTuple partialAggregation) {
        return partialAggregation;
    }

    @Override
    public DoubleTuple add(DoubleTuple partialAggregation, DoubleTuple input) {
        partialAggregation.setValue(partialAggregation.getValue() + input.getValue());
        return partialAggregation;
    }


    @Override
    public void computeLeftCumulativeSliceAggregation(DoubleTuple[] array) {
        for (int i = array.length - 2; i > 0; i--)
            array[i].setValue(array[i].getValue() + array[i + 1].getValue());
    }

    @Override
    public String toString() {
        return "Sum";
    }
}

