package swag;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class KeyedParallelBoundaryAggregator4Doubles extends KeyedProcessFunction<Tuple, Tuple2<Integer, Double>, Tuple2<Integer, DoubleValue>> {
    private transient HashMap<Integer, ParallelBoundaryAggregator> state;
    private int slide, range;
    private transient DoubleValue swag;
    private transient Tuple2<Integer, DoubleValue> result;
    private static ExecutorService executorService;
    private static AtomicInteger cnt;

    public KeyedParallelBoundaryAggregator4Doubles(int slide, int range) {
        this.range = range;
        this.slide = slide;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        state = new HashMap<>();
        swag = new DoubleValue();
        result = new Tuple2<>(0, swag);
        synchronized (Runtime.getRuntime()) {
            if (cnt == null)
                cnt = new AtomicInteger(0);
            if (executorService == null)
                executorService = Executors.newCachedThreadPool();
        }
    }

    @Override
    public void processElement(Tuple2<Integer, Double> value, Context context, Collector<Tuple2<Integer, DoubleValue>> collector) throws Exception {

        ParallelBoundaryAggregator parallelBoundaryAggregator = state.get(value.f0);

        if (parallelBoundaryAggregator == null) {
            parallelBoundaryAggregator = new ParallelBoundaryAggregator(range, slide, executorService);
            state.put(value.f0, parallelBoundaryAggregator);
        }

        parallelBoundaryAggregator.insert(value.f1);
        result.f0 = value.f0;
        swag.setValue(parallelBoundaryAggregator.query());
        collector.collect(result);
    }

    @Override
    public void close() {
        if (cnt.incrementAndGet() == getRuntimeContext().getNumberOfParallelSubtasks()) {
            executorService.shutdown();
            executorService = null;
            cnt = null;
        }
    }
}