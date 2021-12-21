package SlidingWindowAggregations.SlickDeque;

import Aggregations.Aggregation;
import SlidingWindowAggregations.AbstractSlidingWindowAggregation;
import Tuples.PartialAggregation;
import Tuples.StreamingTuple;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;

public abstract class AbstractSlickDequeNonInv<Tuple extends StreamingTuple, SliceAggregation extends PartialAggregation<SliceAggregation>, FinalAggregation> extends AbstractSlidingWindowAggregation<Tuple, SliceAggregation, FinalAggregation> {
    private ArrayDeque<SlickDequeNode> deque;
    private int currentPosition;
    private int numberOfSlicesInEachWindowInstance;
    private Queue<SlickDequeNode> bufferPoolOfSlickDequeNodes; //avoid creating AbstractSlickDeque instances on the fly. The size of the bufferPoolOfSlickDequeNodes corresponds to the space complexity of an algorithm.

    class SlickDequeNode{
        long pos;
        SliceAggregation sliceAggregation;

        SlickDequeNode(int pos, SliceAggregation sliceAggregation) {
            this.pos = pos;
            this.sliceAggregation = sliceAggregation;
        }
    }

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

    /**
     * The condition for max must be explicitly greater than. Because time-based windows may have cases where timestamp is not continuous.
     * For example, in the DEBS'12 Manufacturing Equipment dataset, the timestamp may jump from 2012-02-24T13:00:00.009621100Z to 2012-02-27T01:55:33.968833800Z, which will lead to gaps in time-line.
     * In such a situation, to have correct computation results of sliding window aggregation, the multiple zero values must be inserted into slices of such gaps.
     * For Recal, TwoStack, and BoundaryAggregator, the insertion of slice aggregation is done by each slice.
     * However, for SlickDeque, the insertion will depend on whether the new value v is equals to merge(deque.peekLast(),v), i.e., the result of merging v and the value at the end of the deque.
     * If (v == merge(deque.peekLast(),v)) is true, then the end value of deque will be evict.
     * However, for the gap case of time-base windows, as v is a zero value, if the deque.peekLast() is also a zero value, the condition (v == merge(deque.peekLast(),v)) is true, and the last value of deque will be evicted.
     * In such a case, to ensure that SlickDeque will have correct results, the zero values must be inserted into deque.
     */
    @Override
    public void insert(SliceAggregation sliceAggregation) {
        if (!sliceAggregation.equals(aggregation.creatAccumulator())){ // the reason of this sentence has been explained above.
            while (!deque.isEmpty() && canEvict(sliceAggregation))
                bufferPoolOfSlickDequeNodes.add(deque.removeLast());
        }

//        SlickDequeNode temp = bufferPoolOfSlickDequeNodes.poll();
        SlickDequeNode temp = (bufferPoolOfSlickDequeNodes.isEmpty()) ? new SlickDequeNode(-1, aggregation.creatAccumulator()) : bufferPoolOfSlickDequeNodes.poll();

        temp.pos = currentPosition;
        temp.sliceAggregation = sliceAggregation;
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
        return deque.peekFirst().sliceAggregation;
    }

    @Override
    public void initializeDataStructure() { // initialize deque and currentPosition
        int range = (int)(getRange()), slice = (int)getSlice();
        this.numberOfSlicesInEachWindowInstance = range / slice;
        this.currentPosition = (isRangeMultipleOfSlice()) ? numberOfSlicesInEachWindowInstance - 1 : 0;

        this.deque = new ArrayDeque<>();
        this.deque.addLast(new SlickDequeNode(-1,aggregation.creatAccumulator()));

        bufferPoolOfSlickDequeNodes = new ArrayDeque<>();
        for (int i=0; i<numberOfSlicesInEachWindowInstance-1; i++)
            fillBufferPool(bufferPoolOfSlickDequeNodes);
    }

    abstract void fillBufferPool(Queue<SlickDequeNode> bufferPool);

    @Override
    public String toString() {
        return "AbstractSlickDeque";
    }
}
