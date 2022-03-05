package experiments;

import aggregations.Aggregation;
import aggregations.builtins.MaxInt;
import aggregations.builtins.MomentSketch;
import aggregations.builtins.SlickDequeMomentSketchAgg;
import experiments.util.StreamGenerator;
import streamingtuples.builtins.DoubleTuple;
import streamingtuples.builtins.IntTuple;
import swag.AbstractSlidingWindowAggregation;
import swag.SlidingWindowAggregatorFactory;
import swag.slickdeque.SlickDeque;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

public class Latency {

    public static final String pathToLatencyResult = "./latency/";
    public static final String delimiter = ",";
    private static final Aggregation<IntTuple, IntTuple, IntTuple> aggregation = new MaxInt();
    private static final int sleepTimeInSeconds = 5, testRoundsOfLatency = 1_200_000, repeatOfLatencyExperiment = 50;

    public static void main(String[] args) {
        countBasedLatencyExperiments();
        timeBasedLatencyExperiments();
    }

    private static void countBasedLatencyExperiments() {
        Queue<IntTuple> stream = StreamGenerator.generateStreamOfIntTuples(testRoundsOfLatency, true);
        SlidingWindowAggregatorFactory<IntTuple, IntTuple, IntTuple> factory = new SlidingWindowAggregatorFactory<>();
        int[] ranges = new int[]{(int) Math.pow(2, 13), (int) Math.pow(2, 14)};
        String[] algos = new String[]{"PBA2", "SBA", "SlickDeque", "TwoStack"};
        long[] latency = new long[testRoundsOfLatency];

        for (String algo : algos) {
            for (int range : ranges) {
                for (int i = 0; i < repeatOfLatencyExperiment; i++) {
                    IntTuple[] outputStream = new IntTuple[testRoundsOfLatency];
                    System.out.println(algo + "-" + range + "-" + i);
                    AbstractSlidingWindowAggregation<IntTuple, IntTuple, IntTuple> swag = factory.getSWAG(algo, range, 1, aggregation);
                    swag.computeCountBasedSWAGForLatency(stream, outputStream, latency);

                    if (i >= 5)
                        writeLatencyResults(algo, range, i, latency);

                    try {
                        System.gc();
                        TimeUnit.SECONDS.sleep(sleepTimeInSeconds);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private static void timeBasedLatencyExperiments() {
        Duration sizeOfData = Duration.ofDays(15);
        Duration[] ranges = new Duration[]{Duration.ofHours(1), Duration.ofHours(2)};
        Duration slide = Duration.ofSeconds(1);
        int k = 10;

        Queue<DoubleTuple> stream = StreamGenerator.generateStreamOfDoubleTuplesWithTimeStamp(sizeOfData, 100);
        SlidingWindowAggregatorFactory<DoubleTuple, streamingtuples.builtins.MomentSketch, streamingtuples.builtins.MomentSketch> factory = new SlidingWindowAggregatorFactory<>();
        long[] latency = new long[(int) sizeOfData.getSeconds()];
        String[] algos = new String[]{"PBA2", "SBA", "SlickDeque", "TwoStack"};

        for (String algo : algos) {
            for (Duration range : ranges) {
                for (int i = 0; i < repeatOfLatencyExperiment; i++) {

                    streamingtuples.builtins.MomentSketch[] outputStream = new streamingtuples.builtins.MomentSketch[(int) (sizeOfData.getSeconds() / slide.getSeconds())];
                    System.out.println(algo + "-" + range + "-" + i);
                    AbstractSlidingWindowAggregation<DoubleTuple, streamingtuples.builtins.MomentSketch, streamingtuples.builtins.MomentSketch> swag;
                    if (algo.equalsIgnoreCase("SLICKDEQUE")) {
                        swag = new SlickDeque<>(range, slide, new SlickDequeMomentSketchAgg(k));
                    } else {
                        swag = factory.getSWAGSliceCreation(algo, range, slide, new MomentSketch(k));
                    }

                    swag.computeTimeBasedSWAGForLatency(stream, outputStream, latency);

                    if (i >= 5)
                        writeLatencyResults(algo, (int) range.toHours(), i, latency);

                    try {
                        System.gc();
                        TimeUnit.SECONDS.sleep(sleepTimeInSeconds);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private static void writeLatencyResults(String algo, int range, int expID, long[] latency) {
        String path = pathToLatencyResult + algo + "-" + range + "-" + expID + ".txt";
        PrintWriter printWriter;
        try {
            printWriter = new PrintWriter(new FileOutputStream((new File(path))));
            for (int i = 1, length = latency.length; i <= length; i++) {
                printWriter.append(i + delimiter + latency[i - 1]);
                printWriter.append("\n");
                printWriter.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
