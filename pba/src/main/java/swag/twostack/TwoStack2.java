package swag.twostack;

import aggregations.Aggregation;
import streamingtuples.PartialAggregation;
import streamingtuples.StreamingTuple;

import java.time.Duration;
import java.util.ArrayDeque;

public class TwoStack2<Tuple extends StreamingTuple, SliceAggregation extends PartialAggregation<SliceAggregation>, FinalAggregation> extends AbstractTwoStack<Tuple, SliceAggregation, FinalAggregation> {

    public TwoStack2(int range, int slide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(range, slide, aggregation);
    }

    public TwoStack2(Duration timeRange, Duration timeSlide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(timeRange, timeSlide, aggregation);
    }

//    @Override
//    void mergeWithPartialAggregationInStack(ArrayDeque<TwoStackNode> stack, TwoStackNode twoStackNode) {
//        twoStackNode.accumulatedSliceAggregation.update(twoStackNode.sliceAggregation);
//        twoStackNode.accumulatedSliceAggregation = aggregation.merge(twoStackNode.accumulatedSliceAggregation, getAccumulatedAggregationFromStack(stack));
//    }

    @Override
    void mergeWithPartialAggregationInStack(ArrayDeque<TwoStackNode> stack, TwoStackNode twoStackNode) {
        twoStackNode.accumulatedSliceAggregation = aggregation.createAccumulator();
        twoStackNode.accumulatedSliceAggregation.update(twoStackNode.sliceAggregation);
        twoStackNode.accumulatedSliceAggregation = aggregation.merge(twoStackNode.accumulatedSliceAggregation, getAccumulatedAggregationFromStack(stack));
    }

    @Override
    void fillFrontStackAndBufferPools(long numOfSlices) {
        for (int i = 0; i < numOfSlices; i++) {
            getFrontStack().add(new TwoStackNode(aggregation.createAccumulator(), aggregation.createAccumulator()));
            getBufferPoolOfTwoStackNodes().add(new TwoStackNode(null, null));
        }
    }

    @Override
    public SliceAggregation query() {
        SliceAggregation sliceAggregation = aggregation.createAccumulator();
        sliceAggregation.update(getAccumulatedAggregationFromStack(getFrontStack()));
//        SliceAggregation sliceAggregation = aggregation.merge(aggregation.createAccumulator(), getAccumulatedAggregationFromStack(getFrontStack()));
        sliceAggregation = aggregation.merge(sliceAggregation, getAccumulatedAggregationFromStack(getBackStack()));
        return sliceAggregation;
    }
}