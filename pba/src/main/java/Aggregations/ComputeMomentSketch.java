package Aggregations;

import Tuples.DoubleTuple;
import Tuples.MomentSketch;

public class ComputeMomentSketch implements Aggregation<DoubleTuple, MomentSketch, MomentSketch>{

    private int k;

    public ComputeMomentSketch(int k) {
        this.k = k;
    }

    @Override
    public MomentSketch createAccumulator() {
        return new MomentSketch(k);
    }

    @Override
    public MomentSketch merge(MomentSketch write, MomentSketch read) {
        write.setMin(Math.min(write.getMin(), read.getMin()));
        write.setMax(Math.max(write.getMax(), read.getMax()));
        write.setCount(write.getCount() + read.getCount());

        for (int i=0; i<k; i++){
            write.getMoments()[i] += read.getMoments()[i];
            write.getLogMoments()[i] += read.getLogMoments()[i];
        }
        return write;
    }

    @Override
    public MomentSketch getResult(MomentSketch partialAggregation) {
        return partialAggregation;
    }

    @Override
    public MomentSketch add(MomentSketch partialAggregation, DoubleTuple input) {
        partialAggregation.setMin(Math.min(partialAggregation.getMin(),input.doubleValue()));
        partialAggregation.setMax(Math.max(partialAggregation.getMax(),input.doubleValue()));
        partialAggregation.setCount(partialAggregation.getCount() + 1);

        double v = input.doubleValue();
        double tempV = v;

        double logV = Math.log(v);
        double tempLogV = logV;

        for (int i=0; i<k; i++){
            partialAggregation.getMoments()[i] += tempV;
            partialAggregation.getLogMoments()[i] += tempLogV;

            tempV *= v;
            tempLogV *= logV;
        }

        return partialAggregation;
    }

    @Override
    public void mergeLCS(MomentSketch[] array) {
        MomentSketch write, read;
        for (int i=array.length-2; i>0; i--){
            write = array[i];
            read = array[i+1];
            write.setMin(Math.min(write.getMin(), read.getMin()));
            write.setMax(Math.max(write.getMax(), read.getMax()));
            write.setCount(write.getCount() + read.getCount());

            for (int j=0; j<k; j++){
                write.getMoments()[j] += read.getMoments()[j];
                write.getLogMoments()[j] += read.getLogMoments()[j];
            }
        }

    }
}
