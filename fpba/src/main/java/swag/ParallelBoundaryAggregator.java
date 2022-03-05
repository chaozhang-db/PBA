package swag;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public class ParallelBoundaryAggregator {
    private int chunkSize, range, slide;
    private Queue<double[]> ringBuffer;
    private Queue<double[]> bufferPoolOfSliceAggregationArrays;
    private Queue<AtomicBoolean> ringBufferOfDoneFlag;
    private Queue<AtomicBoolean> bufferPoolOfDoneFlag;
    private double[] sliceAggregationArray, computedLCS;
    private double accumulatedSliceAggregation, chunkAggregation;
    private int sliceIndex;
    private int windowIndex;
    private int marginToChunkBoundary;

    public static ExecutorService threadPool;
    private final LeftCumulativeAggregator leftCumulativeAggregator;

    private class LeftCumulativeAggregator implements Runnable {
        private double[] saa;
        private AtomicBoolean flag;

        @Override
        public void run() {
            for (int i=saa.length-2; i>0; i--){
                saa[i] = Math.max(saa[i], saa[i + 1]);
            }
            flag.set(true);
        }
    }

    public ParallelBoundaryAggregator(int range, int slide, ExecutorService e) {
        this.range = range;
        this.slide = slide;
        initializeDataStructure();
        threadPool = e;
        leftCumulativeAggregator = new LeftCumulativeAggregator();

    }

    private static int computeOptimalChunkSize(int range, int slice){
        int rds = range / slice;
        int rms = range % slice;
        return  (rds <= slice + rms + 1) ? rds : ((slice * (rds + 1) + rms + 1) / (slice + 1));
    }

    private double[] retrieveComputedLCS () {
        AtomicBoolean isDone = ringBufferOfDoneFlag.poll();
        while (!isDone.compareAndSet(true,false)){}
        bufferPoolOfDoneFlag.add(isDone);
        return ringBuffer.poll();
    }

    private void computeLCS(double[] sliceAggregationArray) {
        AtomicBoolean isDone = bufferPoolOfDoneFlag.poll();
        ringBufferOfDoneFlag.add(isDone);

        leftCumulativeAggregator.flag = isDone;
        leftCumulativeAggregator.saa = sliceAggregationArray;
        threadPool.execute(leftCumulativeAggregator);
    }

    public void insert(double sliceAggregation) {
        sliceAggregationArray[sliceIndex++] = sliceAggregation;
        accumulatedSliceAggregation = Math.max(accumulatedSliceAggregation,sliceAggregation);

        if(sliceIndex == chunkSize){
            computeLCS(sliceAggregationArray);

            ringBuffer.add(sliceAggregationArray);
            sliceAggregationArray = bufferPoolOfSliceAggregationArrays.poll();
            chunkAggregation = accumulatedSliceAggregation;
            accumulatedSliceAggregation = Double.NEGATIVE_INFINITY;
            sliceIndex = 0;
        }
    }


    public double query() {
        double sliceAggregation;

        if (windowIndex == 0){
            sliceAggregation = chunkAggregation;
        } else {
            if (windowIndex == 1){
                computedLCS = retrieveComputedLCS();
            }
            sliceAggregation = computedLCS[windowIndex];
        }

        if (windowIndex >= marginToChunkBoundary){
            sliceAggregation = Math.max(sliceAggregation, chunkAggregation);
        }

        sliceAggregation = Math.max(sliceAggregation, accumulatedSliceAggregation);

        if (++windowIndex == chunkSize){ // the current computedLCS is not used any more, such that it will be added into the buffer pool for reusing.
            windowIndex = 0;
            bufferPoolOfSliceAggregationArrays.add(computedLCS); // collecting a used slice aggregation array for future reusing
        }

        return sliceAggregation;
    }

    private double[] creatSAA(){
        double[] saa = new double[chunkSize];
        for (int j=0; j<chunkSize; j++)
            saa[j] = Double.NEGATIVE_INFINITY;
        return saa;
    }


    public void initializeDataStructure() {

        chunkSize = computeOptimalChunkSize(range,slide);

        sliceIndex = 0;

        int rds = range / slide;
        int rms = range % slide;

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

        marginToChunkBoundary = 2 * chunkSize - range / slide;

        chunkAggregation = Double.NEGATIVE_INFINITY;
        accumulatedSliceAggregation = Double.NEGATIVE_INFINITY;

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
