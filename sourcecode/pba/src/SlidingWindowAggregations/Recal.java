package SlidingWindowAggregations;

import Aggregations.*;
import Tuples.PartialAggregation;
import Tuples.StreamingTuple;

import java.time.Duration;
import java.util.ArrayDeque;

public class Recal<Tuple extends StreamingTuple, SliceAggregation extends PartialAggregation<SliceAggregation>, FinalAggregation> extends AbstractSlidingWindowAggregation<Tuple, SliceAggregation, FinalAggregation> {
    private ArrayDeque<SliceAggregation> deque;

    public Recal(int range, int slide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(range, slide, aggregation);
    }

    public Recal(Duration timeRange, Duration timeSlide,  Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(timeRange, timeSlide, aggregation);
    }

    @Override
    public void initializeDataStructure(){
        ArrayDeque<SliceAggregation> deque = new ArrayDeque<>();
        long numOfSlicesInEachWindow = getNumberOfSlicesInEachWindows();

        if (isRangeMultipleOfSlice()) numOfSlicesInEachWindow--;

        for (int i=0; i<numOfSlicesInEachWindow; i++)
           deque.add(aggregation.creatAccumulator());

        this.deque = deque;
    }

    @Override
    public void insert(SliceAggregation sliceAggregation) {
        deque.addLast(sliceAggregation);
    }

    @Override
    public void evict() {
        deque.pollFirst();
    }

    @Override
    public SliceAggregation query() {
        SliceAggregation temp = aggregation.creatAccumulator();
        int i=0;
        for (SliceAggregation sliceAggregation : deque){
            temp = aggregation.merge(temp, sliceAggregation);
            if (++i == getRange()) break;
        }
        return temp;
    }

    @Override
    public String toString() {
        return "Recal";
    }
}
