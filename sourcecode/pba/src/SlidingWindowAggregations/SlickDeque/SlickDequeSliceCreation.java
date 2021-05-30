package SlidingWindowAggregations.SlickDeque;

import SlidingWindowAggregations.AbstractSlidingWindowAggregation;
import Tuples.PartialAggregation;
import Aggregations.SlickDequeAggregation;
import Tuples.StreamingTuple;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Queue;

public class SlickDequeSliceCreation<Tuple extends StreamingTuple, SliceAggregation extends PartialAggregation<SliceAggregation>, FinalAggregation, ACCNI extends PartialAggregation<ACCNI>, OUTNI> extends AbstractSlidingWindowAggregation<Tuple, SliceAggregation, FinalAggregation> {
    SlickDequeAggregation<Tuple, SliceAggregation, FinalAggregation,ACCNI, OUTNI> aggregation;
    ArrayList<SlickDequeNonInvSliceCreation<Tuple,ACCNI,OUTNI>> listOfSlickNonInvDeques;
    SliceAggregation partialAggregation;
    Queue<SliceAggregation> queueOfSliceAgg;
    boolean isEvicted;
    int countOfSlices;

    public SlickDequeSliceCreation(int range, int slide, SlickDequeAggregation<Tuple, SliceAggregation, FinalAggregation,ACCNI, OUTNI> aggregation) {
        super(range, slide, aggregation);

        this.aggregation = aggregation;
        int sizeOfNonInv = aggregation.getNumOfNonInvAgg();
        partialAggregation = aggregation.creatAccumulator();

        queueOfSliceAgg = new ArrayDeque<>();
        listOfSlickNonInvDeques = new ArrayList<>();

        for (int i=0; i<sizeOfNonInv; i++){
            listOfSlickNonInvDeques.add(new SlickDequeNonInvSliceCreation<>(range,slide,aggregation.getNonInvertibleAggregation(i)));
            listOfSlickNonInvDeques.get(i).initializeDataStructure();
        }

        countOfSlices = range / slide;
        isEvicted = false;
    }

    public SlickDequeSliceCreation(Duration timeRange, Duration timeSlide, SlickDequeAggregation<Tuple, SliceAggregation, FinalAggregation,ACCNI, OUTNI> aggregation) {
        super(timeRange, timeSlide, aggregation);

        this.aggregation = aggregation;
        int sizeOfNonInv = aggregation.getNumOfNonInvAgg();
        partialAggregation = aggregation.creatAccumulator();

        listOfSlickNonInvDeques = new ArrayList<>();
        queueOfSliceAgg = new ArrayDeque<>();

        for (int i=0; i<sizeOfNonInv; i++){
            listOfSlickNonInvDeques.add(new SlickDequeNonInvSliceCreation<>(timeRange,timeSlide,aggregation.getNonInvertibleAggregation(i)));
            listOfSlickNonInvDeques.get(i).initializeDataStructure();
        }

        countOfSlices = (int) (timeRange.toMillis() / timeSlide.toMillis());
        isEvicted = false;
    }


    @Override
    public void insert(SliceAggregation sliceAggregation) {
        for (int i=0, length = aggregation.getNumOfNonInvAgg(); i<length; i++)
            listOfSlickNonInvDeques.get(i).insert(aggregation.getElementToInsert(sliceAggregation, i));

        partialAggregation = aggregation.merge(partialAggregation, sliceAggregation);

        queueOfSliceAgg.add(sliceAggregation);

        if (--countOfSlices <= 0)
            isEvicted = true;
    }

    @Override
    public void evict() {
        for (int i=0, length = aggregation.getNumOfNonInvAgg(); i<length; i++)
            listOfSlickNonInvDeques.get(i).evict();

        SliceAggregation evictedAgg = queueOfSliceAgg.poll();

        if (isEvicted){
            partialAggregation = aggregation.applyInverse(partialAggregation,evictedAgg);
        }
    }

    @Override
    public SliceAggregation query() {
        SliceAggregation ret = aggregation.creatAccumulator();
        ret.update(partialAggregation);
        aggregation.setNonInvAggResultToACC(ret,listOfSlickNonInvDeques);
        return ret;
    }

    @Override
    public void initializeDataStructure() { }
}
