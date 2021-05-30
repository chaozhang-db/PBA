package Aggregations;

import Tuples.LongTuple;

public class MaxLongs implements Aggregation<LongTuple, LongTuple, LongTuple> {

    @Override
    public LongTuple creatAccumulator() {
        return LongTuple.MIN_VALUE;
    }

    @Override
    public LongTuple merge(LongTuple write, LongTuple read) {
        return (write.longValue() >= read.longValue())
                ? write
                : read;
    }

    @Override
    public LongTuple getResult(LongTuple partialAggregation) {
        return partialAggregation;
    }

    @Override
    public LongTuple add(LongTuple partialAggregation, LongTuple input) {
        return (partialAggregation.longValue() >= input.longValue())
                ? partialAggregation
                : input;
    }

    @Override
    public void mergeLCS(LongTuple[] array) {
        for (int i=array.length-2; i>0; i--)
            array[i] = (array[i].longValue() >= array[i+1].longValue()) ? array[i] : array[i+1];
    }

    @Override
    public String toString() {
        return "Max";
    }
}