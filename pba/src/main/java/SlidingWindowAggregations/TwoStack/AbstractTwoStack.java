package SlidingWindowAggregations.TwoStack;

import Aggregations.Aggregation;
import SlidingWindowAggregations.AbstractSlidingWindowAggregation;
import Tuples.PartialAggregation;
import Tuples.StreamingTuple;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;

public abstract class AbstractTwoStack<Tuple extends StreamingTuple, SliceAggregation extends PartialAggregation<SliceAggregation>, FinalAggregation> extends AbstractSlidingWindowAggregation<Tuple, SliceAggregation, FinalAggregation> {
    private ArrayDeque<TwoStackNode> frontStack, backStack;
    private Queue<TwoStackNode> bufferPoolOfTwoStackNodes;

    class TwoStackNode {
        SliceAggregation sliceAgg;
        SliceAggregation accumulatedSliceAgg;

        TwoStackNode(SliceAggregation sliceAgg, SliceAggregation accumulatedSliceAgg) {
            this.sliceAgg = sliceAgg;
            this.accumulatedSliceAgg = accumulatedSliceAgg;
        }
    }

    AbstractTwoStack(int range, int slide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(range, slide, aggregation);
    }

    AbstractTwoStack(Duration timeRange, Duration timeSlide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(timeRange, timeSlide, aggregation);
    }

    ArrayDeque<TwoStackNode> getFrontStack() {
        return frontStack;
    }

    ArrayDeque<TwoStackNode> getBackStack() {
        return backStack;
    }

    Queue<TwoStackNode> getBufferPoolOfTwoStackNodes() {
        return bufferPoolOfTwoStackNodes;
    }


    SliceAggregation getAccumulatedAggFromStack(ArrayDeque<TwoStackNode> stack){
        return (stack.isEmpty()) ? aggregation.createAccumulator() : stack.peek().accumulatedSliceAgg;
    }


    /**
     *  Read the accumulated slice aggregation from a stack, and merge it with the slice aggregation of the twoStackNode.
     *  The result is written to the accumulatedSliceAgg of the twoStackNode.
     */
    abstract void combineSliceAggIntoStack(ArrayDeque<TwoStackNode> stack, TwoStackNode twoStackNode);

    @Override
    public void insert(SliceAggregation sliceAggregation) {
        TwoStackNode temp = (bufferPoolOfTwoStackNodes.isEmpty()) ? new TwoStackNode(aggregation.createAccumulator(),aggregation.createAccumulator()) : bufferPoolOfTwoStackNodes.poll();

        temp.sliceAgg = sliceAggregation;
        combineSliceAggIntoStack(backStack, temp);
        backStack.push(temp);
    }

    @Override
    public void evict() {
        if (frontStack.isEmpty()){
            TwoStackNode node;
            while (!backStack.isEmpty()){
                node = backStack.pop();
                combineSliceAggIntoStack(frontStack, node);
                frontStack.push(node);
            }
        }
        bufferPoolOfTwoStackNodes.add(frontStack.pop());
    }

    @Override
    public void initializeDataStructure() {
        long numOfSlices = getNumberOfSlicesInEachWindows();

        this.frontStack = new ArrayDeque<>();
        this.backStack = new ArrayDeque<>();
        this.bufferPoolOfTwoStackNodes = new ArrayDeque<>();

        fillFrontStackAndBufferPools(numOfSlices);

        if (isRangeMultipleOfSlice())
            bufferPoolOfTwoStackNodes.add(frontStack.poll());
    }

    abstract void fillFrontStackAndBufferPools(long numOfSlices);


    @Override
    public String toString() {
        return "TwoStack";
    }
}
