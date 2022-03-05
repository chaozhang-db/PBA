package swag.slickdeque;

import aggregations.Aggregation;
import swag.AbstractSlidingWindowAggregation;
import streamingtuples.PartialAggregation;
import streamingtuples.StreamingTuple;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;

public abstract class AbstractSlickDequeNonInv<Tuple extends StreamingTuple, SliceAggregation extends PartialAggregation<SliceAggregation>, FinalAggregation> extends AbstractSlidingWindowAggregation<Tuple, SliceAggregation, FinalAggregation> {
    private ArrayDeque<SlickDequeNode> deque;
    private int currentPosition;
    private int numberOfSlicesInEachWindowInstance;
    private Queue<SlickDequeNode> bufferPoolOfSlickDequeNodes;

    AbstractSlickDequeNonInv(int range, int slide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(range, slide, aggregation);
    }

    AbstractSlickDequeNonInv(Duration timeRange, Duration timeSlide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(timeRange, timeSlide, aggregation);
    }

    abstract boolean canEvict(SliceAggregation sliceAggregation);

    ArrayDeque<SlickDequeNode> getDeque() {
        return deque;
    }

    @Override
    public void insert(SliceAggregation sliceAggregation) {
        while (!deque.isEmpty() && canEvict(sliceAggregation))
            bufferPoolOfSlickDequeNodes.add(deque.removeLast());

        SlickDequeNode temp;
        if (!bufferPoolOfSlickDequeNodes.isEmpty()) {
            temp = bufferPoolOfSlickDequeNodes.poll();
            temp.pos = currentPosition;
            temp.sliceAggregation = sliceAggregation;
        } else
            temp = new SlickDequeNode(currentPosition, sliceAggregation);

        deque.addLast(temp);
        currentPosition = (++currentPosition) % numberOfSlicesInEachWindowInstance;
    }

    @Override
    public void evict() {
        if (!deque.isEmpty() && (deque.getFirst().pos == currentPosition))
            bufferPoolOfSlickDequeNodes.add(deque.removeFirst());
    }

    @Override
    public SliceAggregation query() {
        return deque.getFirst().sliceAggregation;
    }

    @Override
    public void initializeDataStructure() { // initialize deque and currentPosition
        int range = (int) (getRange()), slice = (int) getSlice();
        this.numberOfSlicesInEachWindowInstance = range / slice;
        this.currentPosition = (isRangeMultipleOfSlice()) ? numberOfSlicesInEachWindowInstance - 1 : 0;

        this.deque = new ArrayDeque<>();
        this.deque.addLast(new SlickDequeNode(currentPosition == 0 ? numberOfSlicesInEachWindowInstance - 1 : currentPosition - 1, aggregation.createAccumulator()));
        bufferPoolOfSlickDequeNodes = new ArrayDeque<>();
    }

    @Override
    public String toString() {
        return "AbstractSlickDeque";
    }

    class SlickDequeNode {
        long pos; // use -1 to indicate an invalid state
        SliceAggregation sliceAggregation;

        SlickDequeNode(int pos, SliceAggregation sliceAggregation) {
            this.pos = pos;
            this.sliceAggregation = sliceAggregation;
        }
    }
}
