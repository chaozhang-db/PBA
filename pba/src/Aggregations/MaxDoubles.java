package Aggregations;

import Tuples.DoubleTuple;

public class MaxDoubles implements Aggregation<DoubleTuple, DoubleTuple, DoubleTuple> {

    @Override
    public DoubleTuple creatAccumulator() {
        return DoubleTuple.MIN_VALUE;
    }

    @Override
    public DoubleTuple merge(DoubleTuple write, DoubleTuple read) {
        return (write.doubleValue() >= read.doubleValue())
                ? write
                : read;
    }

    @Override
    public DoubleTuple getResult(DoubleTuple partialAggregation) {
        return partialAggregation;
    }

    @Override
    public DoubleTuple add(DoubleTuple partialAggregation, DoubleTuple input) {
        return (partialAggregation.doubleValue() >= input.doubleValue())
                ? partialAggregation
                : input;
    }

    @Override
    public void mergeLCS(DoubleTuple[] array) {
        for (int i=array.length-2; i>0; i--)
            array[i] = (array[i].doubleValue() >= array[i+1].doubleValue()) ? array[i] : array[i+1];
    }

    @Override
    public String toString() {
        return "Max";
    }
}
