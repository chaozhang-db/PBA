package Experiments.Util;

import Aggregations.Aggregation;
import SlidingWindowAggregations.AbstractSlidingWindowAggregation;
import SlidingWindowAggregations.SlidingWindowAggregatorFactory;
import Tuples.PartialAggregation;
import Tuples.StreamingTuple;

import java.time.Duration;
import java.util.Queue;

public class TimeBasedThroughputExperiment<Tuple extends StreamingTuple, SliceAggregation extends PartialAggregation<SliceAggregation>, FinalAggregation> extends ThroughputExperiment<Tuple, SliceAggregation, FinalAggregation> {

    private Duration range, slide;

    public TimeBasedThroughputExperiment(String algorithm, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation, Queue<Tuple> inputStream, FinalAggregation[] outputStream, SlidingWindowAggregatorFactory<Tuple, SliceAggregation, FinalAggregation> factory) {
        super(algorithm, aggregation, inputStream, outputStream, factory);
    }


    public Duration getRange() {
        return range;
    }

    public void setRange(Duration range) {
        this.range = range;
    }

    public Duration getSlide() {
        return slide;
    }

    public void setSlide(Duration slide) {
        this.slide = slide;
    }

    @Override
    public void run() {
        AbstractSlidingWindowAggregation<Tuple, SliceAggregation, FinalAggregation> swag = getFactory().getSWAG(getAlgorithm(),getRange(),getSlide(),getAggregation());

        long start = System.nanoTime();
        swag.computeTimeBasedSWAG(getInputStream(),getOutputStream());
        long end = System.nanoTime();
        setExecutionTime((end - start) / 1_000_000);

        setThroughput((double) getInputStream().size() / 1_000 / (double) getExecutionTime());

        System.out.println(toString());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(super.toString());
        sb.append("TimeBasedThroughputExperiment{");
        sb.append("range=").append(range);
        sb.append(", slide=").append(slide);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public String toCSVRecord() {
        final StringBuilder sb = new StringBuilder(super.toCSVRecord());
        sb.append(",").append(range.toMillis());
        sb.append(",").append(slide.toMillis());
        return sb.toString();
    }
}
