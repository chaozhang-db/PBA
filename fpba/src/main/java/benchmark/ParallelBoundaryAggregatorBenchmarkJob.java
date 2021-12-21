package benchmark;

import slidingWindowAggregator.KeyedParallelBoundaryAggregator4Doubles;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.DoubleValue;

public class ParallelBoundaryAggregatorBenchmarkJob {

    ParallelBoundaryAggregatorBenchmarkJob (StreamExecutionEnvironment env, BenchmarkParam benchmarkParam) throws Exception{
        executeSmartHomeBenchmark(env, benchmarkParam);
    }

    private void executeSmartHomeBenchmark (StreamExecutionEnvironment env, BenchmarkParam benchmarkParam) throws Exception {
        DataStream<Tuple2<Integer,Double>> inputStream = env.addSource(new SmartHomeSource(benchmarkParam.getPath(), benchmarkParam.isLocal()));

        inputStream
                .map( new ParallelThroughputLogger<Tuple2<Integer,Double>>(
                        benchmarkParam.getLogFreq(),
                        benchmarkParam.getRange(),
                        "pba",
                        benchmarkParam.getForkID()))
                .keyBy(0)
                .process(new KeyedParallelBoundaryAggregator4Doubles(1, benchmarkParam.getRange()))
                .addSink(new SinkFunction<Tuple2<Integer, DoubleValue>>() {
                    @Override
                    public void invoke(Tuple2<Integer, DoubleValue> value, Context context) throws Exception {
                    }
                });

        String s = "Execution of a pba benchmark job with a range of " + benchmarkParam.getRange();

        env.execute(s);
    }
}
