package Experiments;

import Aggregations.*;
import Experiments.Util.*;
import SlidingWindowAggregations.*;
import SlidingWindowAggregations.BoundaryAggregator.ParallelBoundaryAggregator;
import Tuples.IntTuple;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Throughput {
    private static final String[] algos = new String[]{"PBA", "SBA", "SLICKDEQUE-NONINV", "TwoStack", "Recal"};
    private static final Aggregation<IntTuple, IntTuple, IntTuple> aggregation = new MaxInts();
    private static final SlidingWindowAggregatorFactory<IntTuple, IntTuple, IntTuple> factory = new SlidingWindowAggregatorFactory<>();
    private static final boolean[] isRealData = new boolean[]{false, true};
    private static final ArrayList<ThroughputExperiment<IntTuple, IntTuple, IntTuple>> table = new ArrayList<>();
    private static final int sizeOfRealDataSet = DataLoader.size;

    private static final String pathToThroughputExperimentsResults = "./throughput.txt";
    private static final int repeat = 20, sleepTimeInSeconds = 5, sizeOfSyntheticDataSet = 200_000_000, copyOfRealDataForCountWindow = 16, copyOfRealDataForTimeWindow = 10;
    private static final String pathToRealData = DataLoader.ubuntuPath;

    public static void main(String[] args) {
        throughputExperiments();
    }

    private static void throughputExperiments() {
        setupCountBasedThroughputExperiments();
//        setupTimeBasedThroughputExperiments();
        try {
            PrintWriter printWriter = new PrintWriter(new FileOutputStream((new File(pathToThroughputExperimentsResults))));
            for (ThroughputExperiment<IntTuple, IntTuple, IntTuple> experiment : table) {
                printWriter.append(experiment.toCSVRecord());
                printWriter.append("\n");
                printWriter.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void runThroughputExperiment(String algo, int range, int slide, Queue<IntTuple> stream, IntTuple[] outputStream, boolean isRangeVaried, boolean isRealData) {
        int experimentID = 0;

        int ratioOfRecal = isRangeVaried ? 1024 : 8192;// if range/slice is large, recal is too slow.

        if (!(algo.equalsIgnoreCase("Recal") && (range / slide) > ratioOfRecal)) {
            while (experimentID++ < repeat) {
                CountBasedThroughputExperiment<IntTuple, IntTuple,IntTuple> experiment = new CountBasedThroughputExperiment<>(algo, aggregation, stream, outputStream, factory);
                experiment.setRange(range);
                experiment.setSlide(slide);
                experiment.setTimeBased(false);
                experiment.setRangeVaried(isRangeVaried);
                experiment.setExperimentID(experimentID);
                experiment.setRealData(isRealData);

                experiment.run();
                table.add(experiment);
            }
        }
    }

    private static void runThroughputExperiment(String algo, Duration range, Duration slide, Queue<IntTuple> stream, IntTuple[] outputStream, boolean isRangeVaried, boolean isRealData) {
        int experimentID = 0;

        int ratioOfRecal = isRangeVaried ? 7200 : 14400;// if range/slice is large, recal is too slow.

        if (!(algo.equalsIgnoreCase("Recal") && (range.toMillis() / slide.toMillis()) > ratioOfRecal)) {
            while (experimentID++ < repeat) {
                TimeBasedThroughputExperiment<IntTuple,IntTuple,IntTuple> experiment = new TimeBasedThroughputExperiment<>(algo, aggregation, stream, outputStream, factory);
                experiment.setRange(range);
                experiment.setSlide(slide);
                experiment.setTimeBased(true);
                experiment.setRangeVaried(isRangeVaried);
                experiment.setExperimentID(experimentID);
                experiment.setRealData(isRealData);

                experiment.run();
                table.add(experiment);

                try {
                    System.gc();
                    TimeUnit.SECONDS.sleep(sleepTimeInSeconds);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void setupCountBasedThroughputExperiments() {
        System.out.println("start experiment of count-based windows");

        Queue<IntTuple> stream;
        IntTuple[] outputStream;

        // Comment this sentence in case of using additional datasets
        boolean[] isRealData = new boolean[]{true};

        for (boolean useRealData : isRealData) {
            if (useRealData) {
                stream = DataLoader.load(pathToRealData, false, sizeOfRealDataSet, copyOfRealDataForCountWindow);
                outputStream = new IntTuple[sizeOfRealDataSet * copyOfRealDataForCountWindow];
            } else {
                stream = StreamGenerator.generateStreamOfIntTuples(sizeOfSyntheticDataSet, true);
                outputStream = new IntTuple[sizeOfSyntheticDataSet];
            }

            for (String algo : algos) {
                if (algo.equalsIgnoreCase("PBA"))
                    ParallelBoundaryAggregator.threadPool = Executors.newCachedThreadPool();

                int[] ranges = new int[20 - 3 + 1];
                ranges[0] = 2 * 2 * 2;
                for (int i = 1, length = ranges.length; i < length; i++)// Scenario 1 with slice size of 1, range sizes from 2^3 to 2^{20}
                    ranges[i] = ranges[i - 1] * 2;
                System.out.println("Count-based experiments with a fixed slide of 1 and a range that is one of " + Arrays.toString(ranges));
                for (int range : ranges)
                    runThroughputExperiment(algo, range, 1, stream, outputStream, true, useRealData);

                int[] slides = new int[10];
                for (int i = 0, length = slides.length; i < length; i++)// Scenario 2 with range size of 2^17, slide sizes from 2^1 to 2^10
                    slides[i] = i+1;
                System.out.println("Count-based experiments with a fixed range of 2^17 and a slide that is one of " + Arrays.toString(slides));

                for (int slide : slides){
                    runThroughputExperiment(algo, (int) Math.pow(2, 17), slide, stream, outputStream, false, useRealData);
                }

                if (algo.equalsIgnoreCase("PBA"))
                    ParallelBoundaryAggregator.threadPool.shutdown();
            }
        }
    }

    private static void setupTimeBasedThroughputExperiments() {
        System.out.println("start experiments for time-based windows");

        Queue<IntTuple> stream;
        IntTuple[] outputStream;

        stream = DataLoader.load(pathToRealData, true, sizeOfRealDataSet, copyOfRealDataForTimeWindow);
        outputStream = new IntTuple[sizeOfRealDataSet * copyOfRealDataForTimeWindow];

        String[] algos = new String[]{"PBA", "SBA", "SlickDeque", "TwoStack"};

        for (String algo : algos) {
            if (algo.equalsIgnoreCase("PBA"))
                ParallelBoundaryAggregator.threadPool = Executors.newCachedThreadPool();

            Duration[] ranges = new Duration[10];
            ranges[0] = Duration.ofMinutes(6); // 6 minutes
            for (int i = 1, length = ranges.length; i < length; i++)// Scenario 1 with slice size of 10 ms, ranges from 6 to 60 minutes
                ranges[i] = Duration.ofMinutes(ranges[i-1].toMinutes() + 6);
            System.out.println("Time-based experiments with a fixed slide of" + Duration.ofMillis(10) + "second and a range that is one of " + Arrays.toString(ranges));

            for (Duration range : ranges){
                runThroughputExperiment(algo, range, Duration.ofMillis(10), stream, outputStream, true, true);
            }

            Duration[] slides = new Duration[10];
            slides[0] = Duration.ofMillis(10);
            for (int i = 1, length = slides.length; i < length; i++)// Scenario 2 with range size of 1 hour, slide from 10 ms to 100 ms
                slides[i] = Duration.ofMillis(slides[i - 1].toMillis() + 10);
            System.out.println("Time-based experiments with a fixed range of " + Duration.ofHours(1) + " hours and a slide that is one of " + Arrays.toString(slides));

            for (Duration slide : slides){
                runThroughputExperiment(algo, Duration.ofHours(1), slide, stream, outputStream, false, true);
            }

            if (algo.equalsIgnoreCase("PBA"))
                ParallelBoundaryAggregator.threadPool.shutdown();
        }
    }
}
