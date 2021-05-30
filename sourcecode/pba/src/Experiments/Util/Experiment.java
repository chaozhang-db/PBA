package Experiments.Util;

import Aggregations.Aggregation;
import Tuples.PartialAggregation;
import Tuples.StreamingTuple;
import SlidingWindowAggregations.SlidingWindowAggregatorFactory;

import java.util.Queue;

public abstract class Experiment<Tuple extends StreamingTuple, SliceAggregation extends PartialAggregation<SliceAggregation>, FinalAggregation> {
    private String algorithm;
    private Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation;
    
    private Queue<Tuple> inputStream;
    private FinalAggregation[] outputStream;
    
    private SlidingWindowAggregatorFactory<Tuple, SliceAggregation, FinalAggregation> factory;

    Experiment(String algorithm, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation, Queue<Tuple> inputStream, FinalAggregation[] outputStream, SlidingWindowAggregatorFactory<Tuple, SliceAggregation, FinalAggregation> factory) {
        this.algorithm = algorithm;
        this.aggregation = aggregation;
        this.inputStream = inputStream;
        this.outputStream = outputStream;
        this.factory = factory;
    }

    String getAlgorithm() {
        return algorithm;
    }

    Aggregation<Tuple, SliceAggregation, FinalAggregation> getAggregation() {
        return aggregation;
    }

    Queue<Tuple> getInputStream() {
        return inputStream;
    }

    FinalAggregation[] getOutputStream() {
        return outputStream;
    }

    SlidingWindowAggregatorFactory<Tuple, SliceAggregation, FinalAggregation> getFactory() {
        return factory;
    }

    abstract void run();

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Experiment{");
        sb.append("algorithm='").append(algorithm).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String toCSVRecord() {
        return algorithm;
    }
}
