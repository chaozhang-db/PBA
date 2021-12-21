package Aggregations;

import Tuples.DoubleTuple;

public class MaxDoubleSliceCreation implements Aggregation<DoubleTuple, DoubleTuple, DoubleTuple>{
    @Override
    public DoubleTuple creatAccumulator() { // creates a new slice aggregation instance
        return new DoubleTuple(Double.MIN_VALUE);
    }

    @Override
    public DoubleTuple merge(DoubleTuple write, DoubleTuple read) { // update and return partial aggregation1
        write.setValue(Math.max(write.doubleValue(), read.doubleValue()));
        return write;
    }

    @Override
    public DoubleTuple getResult(DoubleTuple partialAggregation) {
        return partialAggregation;
    }

    @Override
    public DoubleTuple add(DoubleTuple partialAggregation, DoubleTuple input) {
        partialAggregation.setValue(Math.max(partialAggregation.doubleValue(), input.doubleValue()));
        return partialAggregation;
    }


    @Override
    public void mergeLCS(DoubleTuple[] array) {
        for (int i=array.length-2; i>0; i--)
            array[i].setValue(Math.max(array[i].doubleValue(), array[i+1].doubleValue()));
    }

    @Override
    public String toString() {
        return "Max";
    }
}
