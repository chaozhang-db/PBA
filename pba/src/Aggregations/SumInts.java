package Aggregations;

import Tuples.IntTuple;

public class SumInts implements Aggregation<IntTuple, IntTuple, IntTuple> {
    @Override
    public IntTuple creatAccumulator() {
        return new IntTuple(0);
    }

    @Override
    public IntTuple merge(IntTuple write, IntTuple read) {
        write.setValue(write.intValue() + read.intValue());
        return write;
    }

    @Override
    public IntTuple getResult(IntTuple partialAggregation) {
        return partialAggregation;
    }

    @Override
    public IntTuple add(IntTuple partialAggregation, IntTuple input) {
        partialAggregation.setValue(partialAggregation.intValue() + input.intValue());
        return partialAggregation;
    }


    @Override
    public void mergeLCS(IntTuple[] array) {
        for (int i=array.length-2; i>0; i--)
            array[i].setValue(array[i].intValue()+array[i+1].intValue());
    }

    @Override
    public String toString() {
        return "Sum";
    }

}
