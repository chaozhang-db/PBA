package SlidingWindowAggregations.TwoStack;

import Aggregations.Aggregation;
import Tuples.PartialAggregation;
import Tuples.StreamingTuple;

import java.time.Duration;
import java.util.ArrayDeque;

public class TwoStack<Tuple extends StreamingTuple, SliceAggregation extends PartialAggregation<SliceAggregation>, FinalAggregation> extends AbstractTwoStack<Tuple, SliceAggregation, FinalAggregation> {
    public TwoStack(int range, int slide,  Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(range, slide, aggregation);
    }

    public TwoStack(Duration timeRange, Duration timeSlide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(timeRange, timeSlide,  aggregation);
    }

    @Override
    void combineSliceAggIntoStack(ArrayDeque<TwoStackNode> stack, TwoStackNode twoStackNode) {
        twoStackNode.accumulatedSliceAgg = aggregation.merge(twoStackNode.sliceAgg, getAccumulatedAggFromStack(stack));
    }

    @Override
    void fillFrontStackAndBufferPools(long numOfSlices) {
        for (int i=0; i<numOfSlices; i++){
            getFrontStack().add(new TwoStackNode(aggregation.createAccumulator(),aggregation.createAccumulator()));
            getBufferPoolOfTwoStackNodes().add(new TwoStackNode(aggregation.createAccumulator(),aggregation.createAccumulator()));
        }
    }

    @Override
    public SliceAggregation query() {
        return aggregation.merge(getAccumulatedAggFromStack(getFrontStack()), getAccumulatedAggFromStack(getBackStack()));
    }
}
