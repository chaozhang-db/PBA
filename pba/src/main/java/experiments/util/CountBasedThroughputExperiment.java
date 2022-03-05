package experiments.util;

import aggregations.Aggregation;
import swag.AbstractSlidingWindowAggregation;
import swag.SlidingWindowAggregatorFactory;
import streamingtuples.PartialAggregation;
import streamingtuples.StreamingTuple;

import java.util.Queue;

public class CountBasedThroughputExperiment<Tuple extends StreamingTuple, SliceAggregation extends PartialAggregation<SliceAggregation>, FinalAggregation> extends ThroughputExperiment<Tuple, SliceAggregation, FinalAggregation> {

    private int range, slide;

    public CountBasedThroughputExperiment(String algorithm, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation, Queue<Tuple> inputStream, FinalAggregation[] outputStream, SlidingWindowAggregatorFactory<Tuple, SliceAggregation, FinalAggregation> factory) {
        super(algorithm, aggregation, inputStream, outputStream, factory);
    }

    int getRange() {
        return range;
    }

    public void setRange(int range) {
        this.range = range;
    }

    int getSlide() {
        return slide;
    }

    public void setSlide(int slide) {
        this.slide = slide;
    }

    @Override
    public void run() {
        AbstractSlidingWindowAggregation<Tuple, SliceAggregation, FinalAggregation> swag = getFactory().getSWAG(getAlgorithm(), getRange(), getSlide(), getAggregation());

        long start = System.nanoTime();
        swag.computeCountBasedSWAG(getInputStream(), getOutputStream());
        long end = System.nanoTime();
        setExecutionTime((end - start) / 1_000_000);

        setThroughput((double) getInputStream().size() / 1_000 / (double) getExecutionTime());

        System.out.println(toString());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(super.toString());
        sb.append("CountBasedThroughputExperiment{");
        sb.append("range=").append(range);
        sb.append(", slide=").append(slide);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public String toCSVRecord() {
        final StringBuilder sb = new StringBuilder(super.toCSVRecord());
        sb.append(",").append(range);
        sb.append(",").append(slide);
        return sb.toString();
    }
}
