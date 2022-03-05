package swag.boundaryaggregator;

import aggregations.Aggregation;
import streamingtuples.PartialAggregation;
import streamingtuples.StreamingTuple;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public class ParallelBoundaryAggregator<Tuple extends StreamingTuple, SliceAggregation extends PartialAggregation<SliceAggregation>, FinalAggregation> extends AbstractBoundaryAggregator<Tuple, SliceAggregation, FinalAggregation> {
    public static ExecutorService threadPool;
    private final LeftCumulativeAggregator leftCumulativeAggregator;

    private Queue<AtomicBoolean> ringBufferOfFlag;
    private Queue<AtomicBoolean> bufferPoolOfFlag;

    public ParallelBoundaryAggregator(int range, int slide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(range, slide, aggregation);
        leftCumulativeAggregator = new LeftCumulativeAggregator();
    }

    public ParallelBoundaryAggregator(Duration timeRange, Duration timeSlide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        super(timeRange, timeSlide, aggregation);
        leftCumulativeAggregator = new LeftCumulativeAggregator();
    }

    @Override
    SliceAggregation[] retrieveComputedLCS() {
        AtomicBoolean isDone = ringBufferOfFlag.poll();
        bufferPoolOfFlag.add(isDone); // reordering optimization
        while (!isDone.compareAndSet(true, false)) {}
        return ringBuffer.poll();
    }

    @Override
    void computeLcs(SliceAggregation[] sliceAggregationArray) {
        AtomicBoolean isDone = bufferPoolOfFlag.poll();
        ringBufferOfFlag.add(isDone);

        leftCumulativeAggregator.flag = isDone;
        leftCumulativeAggregator.saa = sliceAggregationArray;
        threadPool.execute(leftCumulativeAggregator);
    }

    @Override
    void additionalInitialization(long rds, long rms, long windowIndex, long chunkSize) {
        initializeDoneFlags(rds, rms, windowIndex, chunkSize);
    }

    void initializeDoneFlags(long rds, long rms, long windowIndex, long chunkSize) {
        bufferPoolOfFlag = new ArrayDeque<>(4);
        ringBufferOfFlag = new ArrayDeque<>(4);

        bufferPoolOfFlag.add(new AtomicBoolean(false));
        if (rms == 0) {
            if (rds > chunkSize + 1) ringBufferOfFlag.add(new AtomicBoolean(true));
        } else {
            if (rds > chunkSize) ringBufferOfFlag.add(new AtomicBoolean(true));
        }
        if (windowIndex <= 1) ringBufferOfFlag.add(new AtomicBoolean(true));
    }

    @Override
    public String toString() {
        return "ParallelBoundaryAggregator";
    }

    private class LeftCumulativeAggregator implements Runnable {
        SliceAggregation[] saa;
        AtomicBoolean flag;

        @Override
        public void run() {
            aggregation.computeLeftCumulativeSliceAggregation(saa);
            flag.set(true);
        }
    }
}

