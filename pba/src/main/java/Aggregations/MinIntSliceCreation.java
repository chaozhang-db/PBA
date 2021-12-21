package Aggregations;

import Tuples.IntTuple;

public class MinIntSliceCreation implements Aggregation<IntTuple, IntTuple, IntTuple>{
    @Override
    public IntTuple creatAccumulator() { // creates a new slice aggregation instance
        return new IntTuple(Integer.MAX_VALUE);
    }

    @Override
    public IntTuple merge(IntTuple write, IntTuple read) { // update and return partial aggregation1
        write.setValue(Math.min(write.intValue(), read.intValue()));
        return write;
    }

    @Override
    public IntTuple getResult(IntTuple partialAggregation) {
        return partialAggregation;
    }

    @Override
    public IntTuple add(IntTuple partialAggregation, IntTuple input) {
        partialAggregation.setValue(Math.min(partialAggregation.intValue(), input.intValue()));
        return partialAggregation;
    }


    @Override
    public void mergeLCS(IntTuple[] array) {
        for (int i=array.length-2; i>0; i--)
            array[i].setValue(Math.min(array[i].intValue(), array[i+1].intValue()));
    }

    @Override
    public String toString() {
        return "Min";
    }
}
