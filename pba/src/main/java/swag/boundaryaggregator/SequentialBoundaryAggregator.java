package swag.boundaryaggregator;

import aggregations.Aggregation;
import streamingtuples.PartialAggregation;
import streamingtuples.StreamingTuple;

import java.time.Duration;

public class SequentialBoundaryAggregator<Tuple extends StreamingTuple, SliceAggregation extends PartialAggregation<SliceAggregation>, FinalAggregation> extends AbstractBoundaryAggregator<Tuple, SliceAggregation, FinalAggregation> {

    public SequentialBoundaryAggregator(int range, int slide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(range, slide, aggregation);
    }

    public SequentialBoundaryAggregator(Duration timeRange, Duration timeSlide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(timeRange, timeSlide, aggregation);
    }

    SliceAggregation[] retrieveComputedLCS() {
        return ringBuffer.poll();
    }

    void computeLcs(SliceAggregation[] sliceAggregationArray) {
        aggregation.computeLeftCumulativeSliceAggregation(sliceAggregationArray);
    }

    @Override
    public String toString() {
        return "SequentialBoundaryAggregator";
    }
}
