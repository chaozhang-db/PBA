package swag.slickdeque;

import aggregations.Aggregation;
import streamingtuples.PartialAggregation;
import streamingtuples.StreamingTuple;

import java.time.Duration;

public class SlickDequeNonInv2<Tuple extends StreamingTuple, SliceAggregation extends PartialAggregation<SliceAggregation>, FinalAggregation> extends AbstractSlickDequeNonInv<Tuple, SliceAggregation, FinalAggregation> {

    public SlickDequeNonInv2(int range, int slide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(range, slide, aggregation);
    }

    public SlickDequeNonInv2(Duration timeRange, Duration timeSlide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(timeRange, timeSlide, aggregation);
    }

    @Override
    boolean canEvict(SliceAggregation sliceAggregation) {
        SliceAggregation temp = aggregation.createAccumulator();
        temp.update(getDeque().getLast().sliceAggregation);
        return (aggregation.merge(temp, sliceAggregation).equals(sliceAggregation));
    }
}
