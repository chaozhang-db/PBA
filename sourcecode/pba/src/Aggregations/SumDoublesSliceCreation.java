package Aggregations;

import Tuples.DoubleTuple;

public class SumDoublesSliceCreation implements Aggregation<DoubleTuple, DoubleTuple, DoubleTuple> {
    @Override
    public DoubleTuple creatAccumulator() {
        return new DoubleTuple(0.0);
    }

    @Override
    public DoubleTuple merge(DoubleTuple write, DoubleTuple read) {
        write.setValue(write.doubleValue() + read.doubleValue());
        return write;
    }

    @Override
    public DoubleTuple getResult(DoubleTuple partialAggregation) {
        return partialAggregation;
    }

    @Override
    public DoubleTuple add(DoubleTuple partialAggregation, DoubleTuple input) {
        partialAggregation.setValue(partialAggregation.doubleValue() + input.doubleValue());
        return partialAggregation;
    }


    @Override
    public void mergeLCS(DoubleTuple[] array) {
        for (int i=array.length-2; i>0; i--)
            array[i].setValue(array[i].doubleValue()+array[i+1].doubleValue());
    }

    @Override
    public String toString() {
        return "Sum";
    }
}

