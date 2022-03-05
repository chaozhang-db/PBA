package benchmark;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class FlinkBenchmarkJob {

    FlinkBenchmarkJob (StreamExecutionEnvironment env, BenchmarkParam benchmarkParam) throws Exception{
        executeSmartHomeBenchmark(env, benchmarkParam);
    }

    void executeSmartHomeBenchmark(StreamExecutionEnvironment env, BenchmarkParam benchmarkParam) throws Exception {
        DataStream<Tuple2<Integer,Double>> inputStream = env.addSource(new SmartHomeSource(benchmarkParam.getPath(), benchmarkParam.isLocal()));

        inputStream
                .map(new ParallelThroughputLogger<Tuple2<Integer,Double>>(
                        benchmarkParam.getLogFreq(),
                        benchmarkParam.getRange(),
                        "flink",
                        benchmarkParam.getForkID()))
                .keyBy(0)
                .countWindow(benchmarkParam.getRange(),1)
                .max(1)
                .addSink(new SinkFunction<Tuple2<Integer, Double>>() {
                    @Override
                    public void invoke(Tuple2<Integer, Double> value, Context context) throws Exception { }
                });

        String s = "Execution of a flink benchmark job with a range of " + benchmarkParam.getRange();

        env.execute(s);
    }
}
