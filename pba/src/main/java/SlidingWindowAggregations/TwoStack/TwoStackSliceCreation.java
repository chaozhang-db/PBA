package SlidingWindowAggregations.TwoStack;

import Aggregations.Aggregation;
import Tuples.PartialAggregation;
import Tuples.StreamingTuple;

import java.time.Duration;
import java.util.ArrayDeque;

public class TwoStackSliceCreation<Tuple extends StreamingTuple, SliceAggregation extends PartialAggregation<SliceAggregation>, FinalAggregation> extends AbstractTwoStack<Tuple, SliceAggregation, FinalAggregation> {

    public TwoStackSliceCreation(int range, int slide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(range, slide, aggregation);
    }

    public TwoStackSliceCreation(Duration timeRange, Duration timeSlide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(timeRange, timeSlide, aggregation);
    }

    @Override
    void combineSliceAggIntoStack(ArrayDeque<TwoStackNode> stack, TwoStackNode twoStackNode) {
        twoStackNode.accumulatedSliceAgg.update(twoStackNode.sliceAgg);
        twoStackNode.accumulatedSliceAgg = aggregation.merge(twoStackNode.accumulatedSliceAgg, getAccumulatedAggFromStack(stack)); // update the accumulatedSliceAgg to the value of sliceAggregation
    }

    @Override
    void fillFrontStackAndBufferPools(long numOfSlices) {
        for (int i = 0; i < numOfSlices; i++) {
            getFrontStack().add(new TwoStackNode(aggregation.createAccumulator(),aggregation.createAccumulator()));
            getBufferPoolOfTwoStackNodes().add(new TwoStackNode(null,aggregation.createAccumulator()));
        }
    }

    @Override
    public SliceAggregation query() {
        SliceAggregation sliceAggregation = aggregation.createAccumulator();
        if (!getFrontStack().isEmpty()){
            sliceAggregation.update(getAccumulatedAggFromStack(getFrontStack()));
        }
        sliceAggregation = aggregation.merge(sliceAggregation, getAccumulatedAggFromStack(getBackStack()));
        return sliceAggregation;
    }
}