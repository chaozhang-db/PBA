package benchmark;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class ParallelThroughputLogger<T> extends RichMapFunction <T, T> {
    private long totalReceived = 0;
    private long lastTotalReceived = 0;
    private long lastLogTimeMs = -1;
    private final long logfreq;
    private int executionID;
    private int range;
    private String jobName;
    private int forkID;
    private static volatile boolean isPrinted;


    public ParallelThroughputLogger(long logfreq) {
        this.logfreq = logfreq;
    }

    public ParallelThroughputLogger(long logfreq, int range, String jobName, int forkID) {
        this.logfreq = logfreq;
        this.range = range;
        this.jobName = jobName;
        this.forkID = forkID;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        executionID = 0;
        synchronized (Runtime.getRuntime()){
            if (!isPrinted){
                System.out.println("Fork ID: " + forkID);
                System.out.println("Execution of a " + jobName +" benchmark job with a range of " + range + " and a parallelism of " + getRuntimeContext().getNumberOfParallelSubtasks());
                isPrinted = true;
            }
        }
    }

    @Override
    public void close() throws Exception {
        isPrinted = false;
        System.gc();
    }

    @Override
    public T map(T integerIntegerTuple2) throws Exception {
        totalReceived++;
        if (totalReceived % logfreq == 0) {
            long now = System.currentTimeMillis();
            if (lastLogTimeMs == -1) {
                lastLogTimeMs = now;
                lastTotalReceived = totalReceived;
            } else {
                long timeDiff = now - lastLogTimeMs;
                long elementDiff = totalReceived - lastTotalReceived;
                double ex = (1000 / (double) timeDiff);

                System.out.format("Execution ID: %d. During the last %d ms, we received %d elements in thread %d. That's %f elements/numOfRecordOutPerThread/core.%n",
                        executionID++, timeDiff, elementDiff, getRuntimeContext().getIndexOfThisSubtask(), elementDiff * ex);

                lastLogTimeMs = now;
                lastTotalReceived = totalReceived;
            }
        }
        return integerIntegerTuple2;
    }
}
