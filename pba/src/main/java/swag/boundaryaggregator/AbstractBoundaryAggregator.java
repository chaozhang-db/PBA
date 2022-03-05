package swag.boundaryaggregator;

import aggregations.Aggregation;
import swag.AbstractSlidingWindowAggregation;
import streamingtuples.PartialAggregation;
import streamingtuples.StreamingTuple;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;

public abstract class AbstractBoundaryAggregator<Tuple extends StreamingTuple, SliceAggregation extends PartialAggregation<SliceAggregation>, FinalAggregation> extends AbstractSlidingWindowAggregation<Tuple, SliceAggregation, FinalAggregation> {
    Queue<SliceAggregation[]> ringBuffer;
    private int chunkSize;
    private Queue<SliceAggregation[]> bufferPoolOfSliceAggregationArrays;
    private SliceAggregation[] sliceAggregationArray, computedLCS;
    private SliceAggregation accumulatedSliceAggregation, chunkAggregation;
    private int sliceIndex;
    private int windowIndex;
    private int marginToChunkBoundary;

    public AbstractBoundaryAggregator(int range, int slide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(range, slide, aggregation);
    }

    public AbstractBoundaryAggregator(Duration timeRange, Duration timeSlide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(timeRange, timeSlide, aggregation);
    }

    int computeOptimalChunkSize(int range, int slice) {
        int rds = range / slice;
        int rms = range % slice;
        return (rds <= slice + rms + 1) ? rds : ((slice * (rds + 1) + rms + 1) / (slice + 1));
    }

    int computeOptimalChunkSize(int range, int slice, boolean isTimeBased) {
        // test only, which allows to use the same method to compute optimal chunk size for both count-based and time-based windows
        return isTimeBased ? computeOptimalChunkSize(range / 10, slice / 10) : computeOptimalChunkSize(range, slice);
    }

    abstract SliceAggregation[] retrieveComputedLCS();

    abstract void computeLcs(SliceAggregation[] sliceAggregationArray);

    private SliceAggregation[] createSAA() {
        SliceAggregation[] saa = aggregation.createAccumulator().createPartialAggregationArray(chunkSize);
        for (int j = 0; j < chunkSize; j++)
            saa[j] = aggregation.createAccumulator();
        return saa;
    }

    @Override
    public void insert(SliceAggregation sliceAggregation) {
        sliceAggregationArray[sliceIndex++] = sliceAggregation;
        accumulatedSliceAggregation = aggregation.merge(accumulatedSliceAggregation, sliceAggregation);

        if (sliceIndex == chunkSize) {
            computeLcs(sliceAggregationArray);
            ringBuffer.add(sliceAggregationArray);

            sliceAggregationArray = bufferPoolOfSliceAggregationArrays.poll();
            chunkAggregation = accumulatedSliceAggregation;
            accumulatedSliceAggregation = aggregation.createAccumulator();
            sliceIndex = 0;
        }
    }

    @Override
    public void evict() {
    }

    @Override
    public SliceAggregation query() {
        SliceAggregation sliceAggregation;

        if (windowIndex == 0) {
            sliceAggregation = chunkAggregation;
        } else {
            if (windowIndex == 1) {
                computedLCS = retrieveComputedLCS();
            }
            sliceAggregation = computedLCS[windowIndex];
        }

        if (windowIndex >= marginToChunkBoundary) {
            sliceAggregation = aggregation.merge(sliceAggregation, chunkAggregation);
        }

        sliceAggregation = aggregation.merge(sliceAggregation, accumulatedSliceAggregation);

        if (++windowIndex == chunkSize) { // the current computedLCS is not used anymore, such that it will be added into the buffer pool for reusing.
            windowIndex = 0;
            bufferPoolOfSliceAggregationArrays.add(computedLCS); // collecting a used slice aggregation array for future reusing
        }

        return sliceAggregation;
    }

    @Override
    public void initializeDataStructure() {
        int range = (int) getRange(), slice = (int) getSlice();
        chunkSize = computeOptimalChunkSize(range, slice, isTimeBased());
        sliceIndex = 0;

        int rds = range / slice;
        int rms = range % slice;

        if (rds == chunkSize) {
            if (rms == 0) windowIndex = 1;
            else windowIndex = 0;
        } else if (rds == chunkSize + 1) {
            if (rms == 0) windowIndex = 0;
            else windowIndex = chunkSize - 1;
        } else {
            if (rms == 0) windowIndex = chunkSize - (rds - (chunkSize + 1));
            else windowIndex = 2 * chunkSize - rds;
        }

        marginToChunkBoundary = 2 * chunkSize - range / slice;

        chunkAggregation = aggregation.createAccumulator();
        accumulatedSliceAggregation = aggregation.createAccumulator();

        bufferPoolOfSliceAggregationArrays = new ArrayDeque<>(4);

        for (int i = 0; i < 4; i++)
            bufferPoolOfSliceAggregationArrays.add(createSAA());

        sliceAggregationArray = bufferPoolOfSliceAggregationArrays.poll();

        ringBuffer = new ArrayDeque<>(4);

        ringBuffer.add(bufferPoolOfSliceAggregationArrays.poll());

        if (rms == 0) {
            if (rds > chunkSize + 1) ringBuffer.add(bufferPoolOfSliceAggregationArrays.poll());
        } else if (rds > chunkSize) ringBuffer.add(bufferPoolOfSliceAggregationArrays.poll());

        if (windowIndex > 1) computedLCS = ringBuffer.poll();

        additionalInitialization(rds, rms, windowIndex, chunkSize);
    }

    void additionalInitialization(long rds, long rms, long windowIndex, long chunkSize){};
}

