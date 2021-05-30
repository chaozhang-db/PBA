package Aggregations;

import Tuples.DoubleTuple;

public class MinDoubleSliceCreation implements Aggregation<DoubleTuple, DoubleTuple, DoubleTuple>{
    @Override
    public DoubleTuple creatAccumulator() { // creates a new slice aggregation instance
        return new DoubleTuple(Double.MAX_VALUE);
    }

    @Override
    public DoubleTuple merge(DoubleTuple write, DoubleTuple read) { // update and return partial aggregation1
        write.setValue(Math.min(write.doubleValue(), read.doubleValue()));
        return write;
    }

    @Override
    public DoubleTuple getResult(DoubleTuple partialAggregation) {
        return partialAggregation;
    }

    @Override
    public DoubleTuple add(DoubleTuple partialAggregation, DoubleTuple input) {
        partialAggregation.setValue(Math.min(partialAggregation.doubleValue(), input.doubleValue()));
        return partialAggregation;
    }

    @Override
    public void mergeLCS(DoubleTuple[] array) {
        for (int i=array.length-2; i>0; i--)
            array[i].setValue(Math.min(array[i].doubleValue(), array[i+1].doubleValue()));
    }

    @Override
    public String toString() {
        return "Min";
    }
}