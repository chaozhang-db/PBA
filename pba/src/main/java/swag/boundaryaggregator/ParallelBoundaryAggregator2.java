package swag.boundaryaggregator;

import aggregations.Aggregation;
import streamingtuples.PartialAggregation;
import streamingtuples.StreamingTuple;
import swag.AbstractSlidingWindowAggregation;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ParallelBoundaryAggregator2 is only used to test latency, which can help reduce the overhead to submit a task to thread pool in ParallelBoundaryAggregator.
 *
 * @param <Tuple>
 * @param <SliceAggregation>
 * @param <FinalAggregation>
 */

public class ParallelBoundaryAggregator2<Tuple extends StreamingTuple, SliceAggregation extends PartialAggregation<SliceAggregation>, FinalAggregation> extends AbstractSlidingWindowAggregation<Tuple, SliceAggregation, FinalAggregation> {
    private final AtomicBoolean compute = new AtomicBoolean(false);
    protected Queue<SliceAggregation[]> ringBuffer;
    private int chunkSize;
    private Queue<SliceAggregation[]> bufferPoolOfSliceAggregationArrays; // for reusing sliceAggregationArray
    private SliceAggregation[] sliceAggregationArray, computedLCS;
    private SliceAggregation accumulatedSliceAggregation, chunkAggregation;
    private int sliceIndex;
    private int windowIndex;
    private int marginToChunkBoundary;
    private Thread thread;
    private volatile boolean terminate;
    private volatile SliceAggregation[] lcs;
    private AtomicBoolean isDone;
    private Queue<AtomicBoolean> ringBufferOfDoneFlag;
    private Queue<AtomicBoolean> bufferPoolOfDoneFlag;

    public ParallelBoundaryAggregator2(int range, int slide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(range, slide, aggregation);
        initializeThreadForComputingLCS(aggregation);
    }

    public ParallelBoundaryAggregator2(Duration timeRange, Duration timeSlide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(timeRange, timeSlide, aggregation);
        initializeThreadForComputingLCS(aggregation);
    }

    private void initializeThreadForComputingLCS(Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        terminate = false;
        Runnable computeLCS = () -> {
            while (!terminate) {
                while (!compute.compareAndSet(true, false)) {
                }
                if (terminate)
                    break;
                aggregation.computeLeftCumulativeSliceAggregation(lcs);
                isDone.set(true);
            }
        };

        thread = new Thread(computeLCS, "LeftAccumulativeAggregator");
        thread.setPriority(Thread.MAX_PRIORITY);
        thread.start();
    }


    SliceAggregation[] retrieveComputedLCS() {
        AtomicBoolean isDone = ringBufferOfDoneFlag.poll();
        while (!isDone.compareAndSet(true, false)) {
        }
        bufferPoolOfDoneFlag.add(isDone);
        return ringBuffer.poll();
    }


    void computeLCS(SliceAggregation[] sliceAggregationArray) {
        lcs = sliceAggregationArray;
        isDone = bufferPoolOfDoneFlag.poll();
        ringBufferOfDoneFlag.add(isDone);
        compute.set(true);
    }

    private int computeOptimalChunkSize(int range, int slice) {
        int rds = range / slice;
        int rms = range % slice;
        return (rds <= slice + rms + 1) ? rds : ((slice * (rds + 1) + rms + 1) / (slice + 1));
    }

    private int computeOptimalChunkSize(int range, int slice, boolean isTimeBased) {
        // test only, which allows to use the same method to compute optimal chunk size for both count-based and time-based windows
        return isTimeBased ? computeOptimalChunkSize(range / 10, slice / 10) : computeOptimalChunkSize(range, slice);
    }

    private SliceAggregation[] createSAA() {
        SliceAggregation[] saa = aggregation.createAccumulator().createPartialAggregationArray(chunkSize);
        for (int j = 0; j < chunkSize; j++)
            saa[j] = aggregation.createAccumulator();
        return saa;
    }

    @Override
    public void close() {
        terminate = true;
        compute.set(true);
        try {
            thread.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void insert(SliceAggregation sliceAggregation) {
        sliceAggregationArray[sliceIndex++] = sliceAggregation;
        accumulatedSliceAggregation = aggregation.merge(accumulatedSliceAggregation, sliceAggregation);

        if (sliceIndex == chunkSize) {
            computeLCS(sliceAggregationArray);

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

        if (++windowIndex == chunkSize) { // the current computedLCS is not used any more, such that it will be added into the buffer pool for reusing.
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
            if (rms == 0)
                windowIndex = 1;
            else
                windowIndex = 0;
        } else if (rds == chunkSize + 1) {
            if (rms == 0)
                windowIndex = 0;
            else
                windowIndex = chunkSize - 1;
        } else {
            if (rms == 0)
                windowIndex = chunkSize - (rds - (chunkSize + 1));
            else
                windowIndex = 2 * chunkSize - rds;
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
            if (rds > chunkSize + 1)
                ringBuffer.add(bufferPoolOfSliceAggregationArrays.poll());
        } else {
            if (rds > chunkSize)
                ringBuffer.add(bufferPoolOfSliceAggregationArrays.poll());
        }

        if (windowIndex > 1)
            computedLCS = ringBuffer.poll();

        initializeDoneFlags(rds, rms, windowIndex, chunkSize);// required only by PBA.
    }

    void initializeDoneFlags(long rds, long rms, long windowIndex, long chunkSize) {
        bufferPoolOfDoneFlag = new ArrayDeque<>(4);
        ringBufferOfDoneFlag = new ArrayDeque<>(4);

        bufferPoolOfDoneFlag.add(new AtomicBoolean(false));

        if (rms == 0) {
            if (rds > chunkSize + 1)
                ringBufferOfDoneFlag.add(new AtomicBoolean(true));
        } else {
            if (rds > chunkSize)
                ringBufferOfDoneFlag.add(new AtomicBoolean(true));
        }

        if (windowIndex <= 1)
            ringBufferOfDoneFlag.add(new AtomicBoolean(true));
    }

    @Override
    public String toString() {
        return "ParallelBoundaryAggregator2";
    }
}