package swag;

import aggregations.Aggregation;
import streamingtuples.PartialAggregation;
import streamingtuples.StreamingTuple;

import java.time.Duration;
import java.util.Queue;

/**
 * The implementation of a general sliding window aggregation with a single aggregation and a single pair of range and slide over an in-order input stream.
 */

public abstract class AbstractSlidingWindowAggregation<Tuple extends StreamingTuple, SliceAggregation extends PartialAggregation<SliceAggregation>, FinalAggregation> {
    public final Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation;
    private final long range;
    private final long slide;
    private final long slice; // slice = slide. The idea of determining the size of slices is the same as the one used in the Cutty slicing.
    private final boolean isRangeMultipleOfSlice, isTimeBased;

    public AbstractSlidingWindowAggregation(int range, int slide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        this.range = range;
        this.slide = slide;
        this.slice = slide;
        this.aggregation = aggregation;

        isTimeBased = false;
        isRangeMultipleOfSlice = (range % slice == 0);

        initializeDataStructure();
    }

    public AbstractSlidingWindowAggregation(Duration timeRange, Duration timeSlide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        this.range = timeRange.toMillis();
        this.slide = timeSlide.toMillis();
        this.slice = slide;
        this.aggregation = aggregation;

        isTimeBased = true;
        isRangeMultipleOfSlice = (range % slide == 0);

        initializeDataStructure();
    }

    public void computeTimeBasedSWAG(Queue<Tuple> stream, Queue<FinalAggregation> outputStream) { // assuming that the input stream is in-order, and any of range, slide, and slice is not less than 1 millisecond.
        open();

        if (stream.isEmpty())
            return;

        Tuple first = stream.peek();
        long sliceStartTime = first.getTimeStamp();
        long timeInSlideInterval =
                (range % slide == 0) ?
                        first.getTimeStamp() :
                        (first.getTimeStamp() - (slide - range % slide));
        SliceAggregation sliceAggregation = aggregation.createAccumulator();

        long currentTimeStamp = first.getTimeStamp(), lastTimeStamp;
        long stepSize = (range % slide == 0) ? slide : range % slide;// step size is not larger than a slide or slice, and is required for the case timestamps are not continuous.

        for (Tuple tuple : stream) {
            lastTimeStamp = currentTimeStamp;
            currentTimeStamp = tuple.getTimeStamp();

            if (currentTimeStamp - lastTimeStamp > stepSize) { // progressing lastTimeStamp, i.e., missing timestamps during a period that is more than 1 stepsize
                long progressingTimeStamp = lastTimeStamp + stepSize;

                while (progressingTimeStamp < currentTimeStamp) {// progressing lastTimeStamp until lastTimeStamp == currentTimeStamp - 1.
                    if ((progressingTimeStamp - sliceStartTime) >= slice) {
                        insert(sliceAggregation);
                        sliceAggregation = aggregation.createAccumulator();
                        sliceStartTime += slice;
                    }
                    if ((progressingTimeStamp - timeInSlideInterval) >= slide) {
                        outputStream.add((isRangeMultipleOfSlice) ? aggregation.getResult(query()) : aggregation.getResult(aggregation.merge(query(), sliceAggregation)));
                        evict();
                        timeInSlideInterval += slide;
                    }
                    progressingTimeStamp += stepSize;
                }
            }

            if ((currentTimeStamp - sliceStartTime) >= slice) {
                insert(sliceAggregation);
                sliceAggregation = aggregation.createAccumulator();
                sliceStartTime += slice;
            }

            if ((currentTimeStamp - timeInSlideInterval) >= slide) {
                outputStream.add((isRangeMultipleOfSlice) ? aggregation.getResult(query()) : aggregation.getResult(aggregation.merge(query(), sliceAggregation)));
                evict();
                timeInSlideInterval += slide;
            }

            sliceAggregation = aggregation.add(sliceAggregation, tuple);
        }
        close();
    }

    public void computeTimeBasedSWAG(Queue<Tuple> stream, FinalAggregation[] outputStream) { // for experiments only
        open();

        if (stream.isEmpty())
            return;

        int i = 0;

        Tuple first = stream.peek();
        long sliceStartTime = first.getTimeStamp();
        long timeInSlideInterval =
                (range % slide == 0) ?
                        first.getTimeStamp() :
                        (first.getTimeStamp() - (slide - range % slide));
        SliceAggregation sliceAggregation = aggregation.createAccumulator();

        long currentTimeStamp = first.getTimeStamp(), lastTimeStamp;
        long stepSize = (range % slide == 0) ? slide : range % slide;

        for (Tuple tuple : stream) {
            lastTimeStamp = currentTimeStamp;
            currentTimeStamp = tuple.getTimeStamp();

            if (currentTimeStamp - lastTimeStamp > stepSize) {
                long progressingTimeStamp = lastTimeStamp + stepSize;

                while (progressingTimeStamp < currentTimeStamp) {
                    if ((progressingTimeStamp - sliceStartTime) >= slice) {
                        insert(sliceAggregation);
                        sliceAggregation = aggregation.createAccumulator();
                        sliceStartTime += slice;
                    }
                    if ((progressingTimeStamp - timeInSlideInterval) >= slide) {
                        aggregation.getResult(aggregation.merge(query(), sliceAggregation));//test only
                        evict();
                        timeInSlideInterval += slide;
                    }
                    progressingTimeStamp += stepSize;
                }
            }

            if ((currentTimeStamp - sliceStartTime) >= slice) {
                insert(sliceAggregation);
                sliceAggregation = aggregation.createAccumulator();
                sliceStartTime += slice;
            }

            if ((currentTimeStamp - timeInSlideInterval) >= slide) {
                outputStream[i++] = (isRangeMultipleOfSlice) ? aggregation.getResult(query()) : aggregation.getResult(aggregation.merge(query(), sliceAggregation));
                evict();
                timeInSlideInterval += slide;
            }

            sliceAggregation = aggregation.add(sliceAggregation, tuple);
        }
        close();
    }

    public void computeCountBasedSWAG(Queue<Tuple> stream, Queue<FinalAggregation> outputStream) {
        open();
        int indexInSlice = 0,
                indexInSlideInterval = (range % slide == 0) ? 0 : (int) (slide - range % slice); // the number of steps away from return a final aggregation
        SliceAggregation sliceAggregation = aggregation.createAccumulator();

        for (Tuple tuple : stream) {
            sliceAggregation = aggregation.add(sliceAggregation, tuple);

            indexInSlice++;
            if (indexInSlice == slice) { // a slice ends
                insert(sliceAggregation);
                sliceAggregation = aggregation.createAccumulator();
                indexInSlice = 0;
            }

            indexInSlideInterval++;
            if (indexInSlideInterval == slide) { // a window instance ends
                outputStream.add((isRangeMultipleOfSlice) ? aggregation.getResult(query()) : aggregation.getResult(aggregation.merge(query(), sliceAggregation)));
                evict();
                indexInSlideInterval = 0;
            }
        }
        close();
    }

    public void computeCountBasedSWAG(Queue<Tuple> stream, FinalAggregation[] outputStream) { // for experiment only, which avoids consuming too much memory.
        open();
        int indexInSlice = 0, indexInSlideInterval = (range % slide == 0) ? 0 : (int) (slide - range % slice);
        SliceAggregation sliceAggregation = aggregation.createAccumulator();

        int i = 0;
        for (Tuple tuple : stream) {
            sliceAggregation = aggregation.add(sliceAggregation, tuple);

            indexInSlice++;
            if (indexInSlice == slice) { // a slice ends
                insert(sliceAggregation);
                sliceAggregation = aggregation.createAccumulator();
                indexInSlice = 0;
            }

            indexInSlideInterval++;
            if (indexInSlideInterval == slide) { // a window instance ends
                outputStream[i++] = (isRangeMultipleOfSlice) ? aggregation.getResult(query()) : aggregation.getResult(aggregation.merge(query(), sliceAggregation));
                evict();
                indexInSlideInterval = 0;
            }
        }
        close();
    }

    public void computeCountBasedSWAGForLatency(Queue<Tuple> stream, FinalAggregation[] outputStream, long[] latency) { // for experiment only, which avoids consuming too much memory.
        open();

        int range = (int) this.range, slide = (int) this.slide;

        int indexInSlice = 0, indexInSlideInterval = (range % slide == 0) ? 0 : (slide - range % slide);

        SliceAggregation sliceAggregation = aggregation.createAccumulator();

        int i = 0;

        long start, end;
        int ii = 0;

        for (Tuple tuple : stream) {
            start = System.nanoTime();

            sliceAggregation = aggregation.add(sliceAggregation, tuple);
            indexInSlice++;

            if (indexInSlice == slice) { // a slice ends
                insert(sliceAggregation);
                sliceAggregation = aggregation.createAccumulator();
                indexInSlice = 0;
            }

            indexInSlideInterval++;

            if (indexInSlideInterval == slide) { // a window instance ends
                outputStream[i++] = (isRangeMultipleOfSlice) ? aggregation.getResult(query()) : aggregation.getResult(aggregation.merge(query(), sliceAggregation));
                evict();
                indexInSlideInterval = 0;
            }

            end = System.nanoTime();
            latency[ii++] = end - start;
        }

        close();
    }

    public void computeTimeBasedSWAGForLatency(Queue<Tuple> stream, FinalAggregation[] outputStream, long[] latency) {
        open();

        if (stream.isEmpty())
            return;

        int i = 0;

        Tuple first = stream.peek();
        long sliceStartTime = first.getTimeStamp();
        long timeInSlideInterval =
                (range % slide == 0) ?
                        first.getTimeStamp() :
                        (first.getTimeStamp() - (slide - range % slide));
        SliceAggregation sliceAggregation = aggregation.createAccumulator();

        long currentTimeStamp = first.getTimeStamp(), lastTimeStamp;
        long stepSize = (range % slide == 0) ? slide : range % slide;

        long start, end;
        int ii = 0;

        for (Tuple tuple : stream) {
            start = System.nanoTime();

            lastTimeStamp = currentTimeStamp;
            currentTimeStamp = tuple.getTimeStamp();

            if (currentTimeStamp - lastTimeStamp > stepSize) {
                long progressingTimeStamp = lastTimeStamp + stepSize;

                while (progressingTimeStamp < currentTimeStamp) {
                    if ((progressingTimeStamp - sliceStartTime) >= slice) {
                        insert(sliceAggregation);
                        sliceAggregation = aggregation.createAccumulator();
                        sliceStartTime += slice;
                    }
                    if ((progressingTimeStamp - timeInSlideInterval) >= slide) {
                        aggregation.getResult(aggregation.merge(query(), sliceAggregation));//test only
                        evict();
                        timeInSlideInterval += slide;
                    }
                    progressingTimeStamp += stepSize;
                }
            }

            if ((currentTimeStamp - sliceStartTime) >= slice) {
                insert(sliceAggregation);
                sliceAggregation = aggregation.createAccumulator();
                sliceStartTime += slice;
            }

            if ((currentTimeStamp - timeInSlideInterval) >= slide) {
                outputStream[i++] = (isRangeMultipleOfSlice) ? aggregation.getResult(query()) : aggregation.getResult(aggregation.merge(query(), sliceAggregation));
                evict();
                timeInSlideInterval += slide;

                end = System.nanoTime();
                latency[ii++] = end - start;
            }

            sliceAggregation = aggregation.add(sliceAggregation, tuple);
        }
        close();
    }

    public void close() {
    }

    public void open() {
    }

    public long getRange() {
        return range;
    }

    public long getSlide() {
        return slide;
    }

    public long getSlice() {
        return slice;
    }

    public boolean isRangeMultipleOfSlice() {
        return isRangeMultipleOfSlice;
    }

    public long getNumberOfSlicesInEachWindow() {
        return (getRange() / getSlice());
    }

    public boolean isTimeBased() {
        return isTimeBased;
    }

    public abstract void insert(SliceAggregation sliceAggregation);

    public abstract void evict();

    public abstract SliceAggregation query();

    public abstract void initializeDataStructure();
}
