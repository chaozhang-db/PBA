package SlidingWindowAggregations.SlickDeque;

import Aggregations.Aggregation;
import Tuples.PartialAggregation;
import Tuples.StreamingTuple;

import java.time.Duration;
import java.util.Queue;

public class SlickDequeNonInvSliceCreation<Tuple extends StreamingTuple, SliceAggregation extends PartialAggregation<SliceAggregation>, FinalAggregation>  extends AbstractSlickDequeNonInv<Tuple,SliceAggregation,FinalAggregation> {

    public SlickDequeNonInvSliceCreation(int range, int slide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(range, slide, aggregation);
    }

    public SlickDequeNonInvSliceCreation(Duration timeRange, Duration timeSlide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(timeRange, timeSlide, aggregation);
    }

    @Override
    boolean canEvict(SliceAggregation sliceAggregation) {
        SliceAggregation temp = aggregation.createAccumulator();
        temp.update(getDeque().peekLast().sliceAggregation);
        return (aggregation.merge(temp, sliceAggregation).equals(sliceAggregation));
    }

    @Override
    void fillBufferPool(Queue<SlickDequeNode> bufferPool) {
        bufferPool.add(new SlickDequeNode(-1, null));
    }
}
