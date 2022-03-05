package swag.slickdeque;

import aggregations.Aggregation;
import streamingtuples.PartialAggregation;
import streamingtuples.StreamingTuple;

import java.time.Duration;

// Output references to input streaming values only
public class SlickDequeNonInv<Tuple extends StreamingTuple, SliceAggregation extends PartialAggregation<SliceAggregation>, FinalAggregation> extends AbstractSlickDequeNonInv<Tuple, SliceAggregation, FinalAggregation> {
    public SlickDequeNonInv(int range, int slide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(range, slide, aggregation);
    }

    public SlickDequeNonInv(Duration timeRange, Duration timeSlide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(timeRange, timeSlide, aggregation);
    }

    @Override
    boolean canEvict(SliceAggregation sliceAggregation) {
        return (aggregation.merge(getDeque().getLast().sliceAggregation, sliceAggregation).equals(sliceAggregation));
    }
}
