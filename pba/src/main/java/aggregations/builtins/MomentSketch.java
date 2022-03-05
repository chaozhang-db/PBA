package aggregations.builtins;

import aggregations.Aggregation;
import streamingtuples.builtins.DoubleTuple;

public class MomentSketch implements Aggregation<DoubleTuple, streamingtuples.builtins.MomentSketch, streamingtuples.builtins.MomentSketch> {

    private final int k;

    public MomentSketch(int k) {
        this.k = k;
    }

    @Override
    public streamingtuples.builtins.MomentSketch createAccumulator() {
        return new streamingtuples.builtins.MomentSketch(k);
    }

    @Override
    public streamingtuples.builtins.MomentSketch merge(streamingtuples.builtins.MomentSketch write, streamingtuples.builtins.MomentSketch read) {
        write.setMin(Math.min(write.getMin(), read.getMin()));
        write.setMax(Math.max(write.getMax(), read.getMax()));
        write.setCount(write.getCount() + read.getCount());

        for (int i = 0; i < k; i++) {
            write.getMoments()[i] += read.getMoments()[i];
            write.getLogMoments()[i] += read.getLogMoments()[i];
        }
        return write;
    }

    @Override
    public streamingtuples.builtins.MomentSketch getResult(streamingtuples.builtins.MomentSketch partialAggregation) {
        return partialAggregation;
    }

    @Override
    public streamingtuples.builtins.MomentSketch add(streamingtuples.builtins.MomentSketch partialAggregation, DoubleTuple input) {
        partialAggregation.setMin(Math.min(partialAggregation.getMin(), input.getValue()));
        partialAggregation.setMax(Math.max(partialAggregation.getMax(), input.getValue()));
        partialAggregation.setCount(partialAggregation.getCount() + 1);

        double v = input.getValue();
        double tempV = v;

        double logV = Math.log(v);
        double tempLogV = logV;

        for (int i = 0; i < k; i++) {
            partialAggregation.getMoments()[i] += tempV;
            partialAggregation.getLogMoments()[i] += tempLogV;

            tempV *= v;
            tempLogV *= logV;
        }

        return partialAggregation;
    }

    @Override
    public void computeLeftCumulativeSliceAggregation(streamingtuples.builtins.MomentSketch[] array) {
        streamingtuples.builtins.MomentSketch write, read;
        for (int i = array.length - 2; i > 0; i--) {
            write = array[i];
            read = array[i + 1];
            write.setMin(Math.min(write.getMin(), read.getMin()));
            write.setMax(Math.max(write.getMax(), read.getMax()));
            write.setCount(write.getCount() + read.getCount());

            for (int j = 0; j < k; j++) {
                write.getMoments()[j] += read.getMoments()[j];
                write.getLogMoments()[j] += read.getLogMoments()[j];
            }
        }
    }
}
