package Aggregations;

import Tuples.IntTuple;

public class MinInts implements Aggregation<IntTuple, IntTuple, IntTuple>{
    @Override
    public IntTuple createAccumulator() {
        return IntTuple.MAX_VALUE;
    }

    @Override
    public IntTuple merge(IntTuple write, IntTuple read) {
        return (write.intValue() <= read.intValue())
                ? write
                : read;
    }

    @Override
    public IntTuple getResult(IntTuple partialAggregation) {
        return partialAggregation;
    }

    @Override
    public IntTuple add(IntTuple partialAggregation, IntTuple input) {
        return (partialAggregation.intValue() <= input.intValue())
                ? partialAggregation
                : input;
    }


    @Override
    public void mergeLCS(IntTuple[] array) {
        for (int i=array.length-2; i>0; i--)
            array[i] = (array[i].intValue() <= array[i+1].intValue()) ? array[i] : array[i+1];
    }
}
