package SlidingWindowAggregations.BoundaryAggregator;

import Aggregations.Aggregation;
import SlidingWindowAggregations.AbstractSlidingWindowAggregation;
import Tuples.PartialAggregation;
import Tuples.StreamingTuple;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public class ParallelBoundaryAggregator<Tuple extends StreamingTuple, SliceAggregation extends PartialAggregation<SliceAggregation>, FinalAggregation> extends AbstractSlidingWindowAggregation<Tuple, SliceAggregation, FinalAggregation> {
    private int chunkSize;
    private Queue<SliceAggregation[]> ringBuffer;
    private Queue<SliceAggregation[]> bufferPoolOfSliceAggregationArrays;
    private Queue<AtomicBoolean> ringBufferOfDoneFlag;
    private Queue<AtomicBoolean> bufferPoolOfDoneFlag;
    private SliceAggregation[] sliceAggregationArray, computedLCS;
    private SliceAggregation accumulatedSliceAggregation, chunkAggregation;
    private int sliceIndex;
    private int windowIndex;
    private int marginToChunkBoundary;

    public static ExecutorService threadPool;
    private final LeftCumulativeAggregator leftCumulativeAggregator;

    private class LeftCumulativeAggregator implements Runnable {
        private SliceAggregation[] saa;
        private AtomicBoolean flag;

        @Override
        public void run() {
            aggregation.mergeLCS(saa);
            flag.set(true);
        }
    }

    public ParallelBoundaryAggregator(int range, int slide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(range, slide,  aggregation);
        leftCumulativeAggregator = new LeftCumulativeAggregator();

    }

    public ParallelBoundaryAggregator(Duration timeRange, Duration timeSlide,  Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(timeRange, timeSlide, aggregation);
        leftCumulativeAggregator = new LeftCumulativeAggregator();
    }

    private static int computeOptimalChunkSize(int range, int slice){
        int rds = range / slice;
        int rms = range % slice;
        return  (rds <= slice + rms + 1) ? rds : ((slice * (rds + 1) + rms + 1) / (slice + 1));
    }
    static int computeOptimalChunkSize(int range, int slice, boolean isTimeBased){
        // test only, which allows to use the same method to compute optimal chunk size for both count-based and time-based windows
        return isTimeBased ? computeOptimalChunkSize(range/10, slice/10) : computeOptimalChunkSize(range,slice);
    }

    private SliceAggregation[] retrieveComputedLCS () {
        AtomicBoolean isDone = ringBufferOfDoneFlag.poll();
        while (!isDone.compareAndSet(true,false)){}
        bufferPoolOfDoneFlag.add(isDone);
        return ringBuffer.poll();
    }

    private void computeLCS(SliceAggregation[] sliceAggregationArray) {
        AtomicBoolean isDone = bufferPoolOfDoneFlag.poll();
        ringBufferOfDoneFlag.add(isDone);

        leftCumulativeAggregator.flag = isDone;
        leftCumulativeAggregator.saa = sliceAggregationArray;
        threadPool.execute(leftCumulativeAggregator);
    }

    @Override
    public void insert(SliceAggregation sliceAggregation) {
        sliceAggregationArray[sliceIndex++] = sliceAggregation;
        accumulatedSliceAggregation = aggregation.merge(accumulatedSliceAggregation,sliceAggregation);

        if(sliceIndex == chunkSize){
            computeLCS(sliceAggregationArray);

            ringBuffer.add(sliceAggregationArray);
            sliceAggregationArray = bufferPoolOfSliceAggregationArrays.poll();
            chunkAggregation = accumulatedSliceAggregation;
            accumulatedSliceAggregation = aggregation.creatAccumulator();
            sliceIndex = 0;
        }
    }

    @Override
    public void evict() {}

    @Override
    public SliceAggregation query() {
        SliceAggregation sliceAggregation;

        if (windowIndex == 0){
            sliceAggregation = chunkAggregation;
        } else {
            if (windowIndex == 1){
                computedLCS = retrieveComputedLCS();
            }
            sliceAggregation = computedLCS[windowIndex];
        }

        if (windowIndex >= marginToChunkBoundary){
            sliceAggregation = aggregation.merge(sliceAggregation, chunkAggregation);
        }

        sliceAggregation = aggregation.merge(sliceAggregation, accumulatedSliceAggregation);

        if (++windowIndex == chunkSize){ // the current computedLCS is not used any more, such that it will be added into the buffer pool for reusing.
            windowIndex = 0;
            bufferPoolOfSliceAggregationArrays.add(computedLCS); // collecting a used slice aggregation array for future reusing
        }

        return sliceAggregation;
    }

    private SliceAggregation[] creatSAA(){
        SliceAggregation[] saa = aggregation.creatAccumulator().createArrayOfACC(chunkSize);
        for (int j=0; j<chunkSize; j++)
            saa[j] = aggregation.creatAccumulator();
        return saa;
    }

    @Override
    public void initializeDataStructure() {
        int range = (int) getRange(), slice = (int) getSlice();

        chunkSize = computeOptimalChunkSize(range,slice,isTimeBased());

        sliceIndex = 0;

        int rds = range / slice;
        int rms = range % slice;

        if (rds == chunkSize){
            if (rms == 0)
                windowIndex = 1;
            else
                windowIndex = 0;
        } else if (rds == chunkSize + 1){
            if (rms == 0)
                windowIndex = 0;
            else
                windowIndex = chunkSize - 1;
        } else{
            if (rms == 0)
                windowIndex = chunkSize - (rds - (chunkSize + 1));
            else
                windowIndex = 2 * chunkSize - rds;
        }

        marginToChunkBoundary = 2 * chunkSize - range / slice;

        chunkAggregation = aggregation.creatAccumulator();
        accumulatedSliceAggregation = aggregation.creatAccumulator();

        bufferPoolOfSliceAggregationArrays = new ArrayDeque<>(4);


        for (int i=0; i<4; i++)
            bufferPoolOfSliceAggregationArrays.add(creatSAA());

        sliceAggregationArray = bufferPoolOfSliceAggregationArrays.poll();

        ringBuffer = new ArrayDeque<>(4);

        ringBuffer.add(bufferPoolOfSliceAggregationArrays.poll());

        if (rms == 0){
            if (rds > chunkSize + 1)
                ringBuffer.add(bufferPoolOfSliceAggregationArrays.poll());
        } else
        if (rds > chunkSize)
            ringBuffer.add(bufferPoolOfSliceAggregationArrays.poll());

        if (windowIndex > 1)
            computedLCS = ringBuffer.poll();

        initializeDoneFlags(rds,rms,windowIndex, chunkSize);// required only by PBA.
    }

    void initializeDoneFlags(long rds, long rms, long windowIndex, long chunkSize) {
        bufferPoolOfDoneFlag = new ArrayDeque<>(4);
        ringBufferOfDoneFlag = new ArrayDeque<>(4);


        bufferPoolOfDoneFlag.add(new AtomicBoolean(false));
        if (rms == 0){
            if (rds > chunkSize + 1)
                ringBufferOfDoneFlag.add(new AtomicBoolean(true));
        } else{
            if (rds > chunkSize)
                ringBufferOfDoneFlag.add(new AtomicBoolean(true));
        }
        if (windowIndex <= 1)
            ringBufferOfDoneFlag.add(new AtomicBoolean(true));
    }

    @Override
    public String toString() {
        return "ParallelBoundaryAggregator";
    }
}

