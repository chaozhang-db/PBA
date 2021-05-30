package Experiments.Util;

import Aggregations.Aggregation;
import Tuples.PartialAggregation;
import Tuples.StreamingTuple;
import SlidingWindowAggregations.SlidingWindowAggregatorFactory;
import java.util.Queue;

public abstract class ThroughputExperiment<Tuple extends StreamingTuple, SliceAggregation extends PartialAggregation<SliceAggregation>, FinalAggregation>  extends Experiment<Tuple, SliceAggregation, FinalAggregation> {
    private boolean isRealData;
    private boolean isTimeBased;
    private boolean isRangeVaried;
    private long executionTime;
    private int experimentID;
    private double throughput;


    public ThroughputExperiment(String algorithm, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation, Queue<Tuple> inputStream, FinalAggregation[] outputStream, SlidingWindowAggregatorFactory<Tuple, SliceAggregation, FinalAggregation> factory) {
        super(algorithm, aggregation, inputStream, outputStream, factory);
    }

    double getThroughput() {
        return throughput;
    }

    void setThroughput(double throughput) {
        this.throughput = throughput;
    }

    long getExecutionTime() {
        return executionTime;
    }

    void setExecutionTime(long executionTime) {
        this.executionTime = executionTime;
    }

    int getExperimentID() {
        return experimentID;
    }

    public void setExperimentID(int experimentID) {
        this.experimentID = experimentID;
    }

    public boolean isRangeVaried() {
        return isRangeVaried;
    }

    public void setRangeVaried(boolean rangeVaried) {
        isRangeVaried = rangeVaried;
    }

    public boolean isTimeBased() {
        return isTimeBased;
    }

    public void setTimeBased(boolean timeBased) {
        isTimeBased = timeBased;
    }

    public boolean isRealData() {
        return isRealData;
    }

    public void setRealData(boolean realData) {
        isRealData = realData;
    }

    @Override
    public String toCSVRecord(){
        final StringBuilder sb = new StringBuilder(super.toCSVRecord());
        sb.append(",").append(isRealData);
        sb.append(",").append(isTimeBased);
        sb.append(",").append(isRangeVaried);
        sb.append(",").append(executionTime);
        sb.append(",").append(experimentID);
        sb.append(",").append(throughput);
        return sb.toString();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(super.toString());
        sb.append(("ThroughputExperiment{"));
        sb.append("isRealData=").append(isRealData);
        sb.append(", isTimeBased=").append(isTimeBased);
        sb.append(", isRangeVaried=").append(isRangeVaried);
        sb.append(", executionTime=").append(executionTime);
        sb.append(", experimentID=").append(experimentID);
        sb.append(", throughput=").append(throughput);
        sb.append('}');
        return sb.toString();
    }
}
