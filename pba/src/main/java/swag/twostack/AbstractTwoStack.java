package swag.twostack;

import aggregations.Aggregation;
import swag.AbstractSlidingWindowAggregation;
import streamingtuples.PartialAggregation;
import streamingtuples.StreamingTuple;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;

public abstract class AbstractTwoStack<Tuple extends StreamingTuple, SliceAggregation extends PartialAggregation<SliceAggregation>, FinalAggregation> extends AbstractSlidingWindowAggregation<Tuple, SliceAggregation, FinalAggregation> {
    private ArrayDeque<TwoStackNode> frontStack, backStack;
    private Queue<TwoStackNode> bufferPoolOfTwoStackNodes;

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

    SliceAggregation getAccumulatedAggregationFromStack(ArrayDeque<TwoStackNode> stack) {
        return (stack.isEmpty()) ? aggregation.createAccumulator() : stack.peek().accumulatedSliceAggregation;
    }

    /**
     * Read the current accumulated slice aggregation from a stack, and merge it with the slice aggregation of the TwoStack Node.
     * The result is written to the accumulatedSliceAggregation of the twoStackNode.
     */
    abstract void mergeWithPartialAggregationInStack(ArrayDeque<TwoStackNode> stack, TwoStackNode twoStackNode);

    @Override
    public void insert(SliceAggregation sliceAggregation) {
        TwoStackNode temp = (bufferPoolOfTwoStackNodes.isEmpty()) ? new TwoStackNode(null, null) : bufferPoolOfTwoStackNodes.poll();
        temp.sliceAggregation = sliceAggregation;
        mergeWithPartialAggregationInStack(backStack, temp);
        backStack.push(temp);
    }

    @Override
    public void evict() {
        if (frontStack.isEmpty()) {
            TwoStackNode node;
            while (!backStack.isEmpty()) {
                node = backStack.pop();
                mergeWithPartialAggregationInStack(frontStack, node);
                frontStack.push(node);
            }
        }
        bufferPoolOfTwoStackNodes.add(frontStack.pop());
    }

    @Override
    public void initializeDataStructure() {
        this.frontStack = new ArrayDeque<>();
        this.backStack = new ArrayDeque<>();
        this.bufferPoolOfTwoStackNodes = new ArrayDeque<>();

        long numOfSlices = getNumberOfSlicesInEachWindow();
        fillFrontStackAndBufferPools(numOfSlices);

        if (isRangeMultipleOfSlice())
            bufferPoolOfTwoStackNodes.add(frontStack.poll());
    }

    abstract void fillFrontStackAndBufferPools(long numOfSlices);

    @Override
    public String toString() {
        return "TwoStack";
    }

    class TwoStackNode {
        SliceAggregation sliceAggregation;
        SliceAggregation accumulatedSliceAggregation;

        TwoStackNode(SliceAggregation sliceAggregation, SliceAggregation accumulatedSliceAggregation) {
            this.sliceAggregation = sliceAggregation;
            this.accumulatedSliceAggregation = accumulatedSliceAggregation;
        }
    }
}
