package swag.slickdeque;

import aggregations.builtins.SlickDequeAggregation;
import swag.AbstractSlidingWindowAggregation;
import streamingtuples.PartialAggregation;
import streamingtuples.StreamingTuple;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Queue;

public class SlickDeque<Tuple extends StreamingTuple, SliceAggregation extends PartialAggregation<SliceAggregation>, FinalAggregation, ACCNI extends PartialAggregation<ACCNI>, OUTNI> extends AbstractSlidingWindowAggregation<Tuple, SliceAggregation, FinalAggregation> {
    SlickDequeAggregation<Tuple, SliceAggregation, FinalAggregation, ACCNI, OUTNI> aggregation;
    ArrayList<SlickDequeNonInv2<Tuple, ACCNI, OUTNI>> listOfSlickNonInvDequeues;
    SliceAggregation partialAggregation;
    Queue<SliceAggregation> queueOfSliceAgg;
    boolean isEvicted;
    int countOfSlices;

    public SlickDeque(int range, int slide, SlickDequeAggregation<Tuple, SliceAggregation, FinalAggregation, ACCNI, OUTNI> aggregation) {
        super(range, slide, aggregation);

        this.aggregation = aggregation;
        int sizeOfNonInv = aggregation.getNumOfNonInvAgg();
        partialAggregation = aggregation.createAccumulator();

        queueOfSliceAgg = new ArrayDeque<>();
        listOfSlickNonInvDequeues = new ArrayList<>();

        for (int i = 0; i < sizeOfNonInv; i++) {
            listOfSlickNonInvDequeues.add(new SlickDequeNonInv2<>(range, slide, aggregation.getNonInvertibleAggregation(i)));
            listOfSlickNonInvDequeues.get(i).initializeDataStructure();
        }

        countOfSlices = range / slide;
        isEvicted = false;
    }

    public SlickDeque(Duration timeRange, Duration timeSlide, SlickDequeAggregation<Tuple, SliceAggregation, FinalAggregation, ACCNI, OUTNI> aggregation) {
        super(timeRange, timeSlide, aggregation);

        this.aggregation = aggregation;
        int sizeOfNonInv = aggregation.getNumOfNonInvAgg();
        partialAggregation = aggregation.createAccumulator();

        listOfSlickNonInvDequeues = new ArrayList<>();
        queueOfSliceAgg = new ArrayDeque<>();

        for (int i = 0; i < sizeOfNonInv; i++) {
            listOfSlickNonInvDequeues.add(new SlickDequeNonInv2<>(timeRange, timeSlide, aggregation.getNonInvertibleAggregation(i)));
            listOfSlickNonInvDequeues.get(i).initializeDataStructure();
        }

        countOfSlices = (int) (timeRange.toMillis() / timeSlide.toMillis());
        isEvicted = false;
    }


    @Override
    public void insert(SliceAggregation sliceAggregation) {
        for (int i = 0, length = aggregation.getNumOfNonInvAgg(); i < length; i++)
            listOfSlickNonInvDequeues.get(i).insert(aggregation.getElementToInsert(sliceAggregation, i));

        partialAggregation = aggregation.merge(partialAggregation, sliceAggregation);

        queueOfSliceAgg.add(sliceAggregation);

        if (--countOfSlices <= 0)
            isEvicted = true;
    }

    @Override
    public void evict() {
        for (int i = 0, length = aggregation.getNumOfNonInvAgg(); i < length; i++)
            listOfSlickNonInvDequeues.get(i).evict();

        SliceAggregation evictedAgg = queueOfSliceAgg.poll();

        if (isEvicted) {
            partialAggregation = aggregation.applyInverse(partialAggregation, evictedAgg);
        }
    }

    @Override
    public SliceAggregation query() {
        SliceAggregation ret = aggregation.createAccumulator();
        ret.update(partialAggregation);
        aggregation.setNonInvAggResultToACC(ret, listOfSlickNonInvDequeues);
        return ret;
    }

    @Override
    public void initializeDataStructure() {
    }
}
