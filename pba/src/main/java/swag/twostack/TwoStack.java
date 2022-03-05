package swag.twostack;

import aggregations.Aggregation;
import streamingtuples.PartialAggregation;
import streamingtuples.StreamingTuple;

import java.time.Duration;
import java.util.ArrayDeque;

// Output references to input streaming values only
public class TwoStack<Tuple extends StreamingTuple, SliceAggregation extends PartialAggregation<SliceAggregation>, FinalAggregation> extends AbstractTwoStack<Tuple, SliceAggregation, FinalAggregation> {
    public TwoStack(int range, int slide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(range, slide, aggregation);
    }

    public TwoStack(Duration timeRange, Duration timeSlide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(timeRange, timeSlide, aggregation);
    }

    @Override
    void mergeWithPartialAggregationInStack(ArrayDeque<TwoStackNode> stack, TwoStackNode twoStackNode) {
        twoStackNode.accumulatedSliceAggregation = aggregation.merge(twoStackNode.sliceAggregation, getAccumulatedAggregationFromStack(stack));
    }

    @Override
    void fillFrontStackAndBufferPools(long numOfSlices) {
        for (int i = 0; i < numOfSlices; i++) {
            getFrontStack().add(new TwoStackNode(aggregation.createAccumulator(), aggregation.createAccumulator()));
            getBufferPoolOfTwoStackNodes().add(new TwoStackNode(aggregation.createAccumulator(), aggregation.createAccumulator()));
        }
    }

    @Override
    public SliceAggregation query() {
        return aggregation.merge(getAccumulatedAggregationFromStack(getFrontStack()), getAccumulatedAggregationFromStack(getBackStack()));
    }
}
