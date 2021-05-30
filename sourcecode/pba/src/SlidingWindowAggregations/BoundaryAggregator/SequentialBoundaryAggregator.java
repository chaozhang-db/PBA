package SlidingWindowAggregations.BoundaryAggregator;

import Aggregations.Aggregation;
import SlidingWindowAggregations.AbstractSlidingWindowAggregation;
import Tuples.PartialAggregation;
import Tuples.StreamingTuple;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;

import static SlidingWindowAggregations.BoundaryAggregator.ParallelBoundaryAggregator.computeOptimalChunkSize;

public class SequentialBoundaryAggregator<Tuple extends StreamingTuple, SliceAggregation extends PartialAggregation<SliceAggregation>, FinalAggregation> extends AbstractSlidingWindowAggregation<Tuple, SliceAggregation, FinalAggregation> {
    private int chunkSize;
    protected Queue<SliceAggregation[]> ringBuffer;
    private Queue<SliceAggregation[]> bufferPoolOfSliceAggregationArrays; // for reusing sliceAggregationArray
    private SliceAggregation[] sliceAggregationArray, computedLCS;
    private SliceAggregation accumulatedSliceAggregation, chunkAggregation;
    private int sliceIndex;
    private int windowIndex;
    private int marginToChunkBoundary;

    public SequentialBoundaryAggregator(int range, int slide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(range, slide, aggregation);
    }

    public SequentialBoundaryAggregator(Duration timeRange, Duration timeSlide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(timeRange, timeSlide, aggregation);
    }

    SliceAggregation[] retrieveComputedLCS() {
        return ringBuffer.poll();
    }

    void computeLCS(SliceAggregation[] sliceAggregationArray) {
        aggregation.mergeLCS(sliceAggregationArray);
    }

    private SliceAggregation[] creatSAA(){
        SliceAggregation[] saa = aggregation.creatAccumulator().createArrayOfACC(chunkSize);
        for (int j=0; j<chunkSize; j++)
            saa[j] = aggregation.creatAccumulator();
        return saa;
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
    public void evict() {

    }

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
    }

    @Override
    public String toString() {
        return "SequentialBoundaryAggregator";
    }
}
