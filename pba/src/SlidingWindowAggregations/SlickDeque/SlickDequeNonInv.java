package SlidingWindowAggregations.SlickDeque;

import Aggregations.Aggregation;
import Tuples.PartialAggregation;
import Tuples.StreamingTuple;

import java.time.Duration;
import java.util.Queue;

public class SlickDequeNonInv<Tuple extends StreamingTuple, SliceAggregation extends PartialAggregation<SliceAggregation>, FinalAggregation> extends AbstractSlickDequeNonInv<Tuple, SliceAggregation, FinalAggregation> {
    public SlickDequeNonInv(int range, int slide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(range, slide, aggregation);
    }

    public SlickDequeNonInv(Duration timeRange, Duration timeSlide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(timeRange, timeSlide,  aggregation);
    }

    @Override
    boolean canEvict(SliceAggregation sliceAggregation) {
        return (aggregation.merge(getDeque().peekLast().sliceAggregation,sliceAggregation).equals(sliceAggregation));
    }

    @Override
    void fillBufferPool(Queue<SlickDequeNode> bufferPool) {
        bufferPool.add(new SlickDequeNode(-1, aggregation.creatAccumulator()));
    }
}
