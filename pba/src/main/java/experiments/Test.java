package experiments;

import aggregations.Aggregation;
import aggregations.builtins.MaxInt;
import experiments.util.StreamGenerator;
import streamingtuples.PartialAggregation;
import streamingtuples.StreamingTuple;
import streamingtuples.builtins.IntTuple;
import swag.AbstractSlidingWindowAggregation;
import swag.Recal;
import swag.boundaryaggregator.ParallelBoundaryAggregator;
import swag.boundaryaggregator.SequentialBoundaryAggregator;
import swag.slickdeque.SlickDequeNonInv;
import swag.slickdeque.SlickDequeNonInv2;
import swag.twostack.TwoStack;
import swag.twostack.TwoStack2;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.Executors;

public class Test<Tuple extends StreamingTuple, SliceAggregation extends PartialAggregation<SliceAggregation>, FinalAggregation> {

    private final Queue<Tuple> stream;
    private final Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation;
    private final boolean isCountCased;
    private final boolean isSliceCreated;

    public Test(Queue<Tuple> stream, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation, boolean isCountCased, boolean isSliceCreated) {
        this.stream = stream;
        this.aggregation = aggregation;
        this.isCountCased = isCountCased;
        this.isSliceCreated = isSliceCreated;
    }

    public static String getAlgo(int id) {
        String s;
        switch (id) {
            case 1:
                s = "TS";
                break;
            case 2:
                s = "SD";
                break;
            case 3:
                s = "SBA";
                break;
            default:
                s = "PBA";
        }
        return s;
    }

    public static void main(String[] args) {
        Test<IntTuple, IntTuple, IntTuple> test = new Test<>(
                StreamGenerator.generateStreamOfIntTuples(5_000_000, true),
                new MaxInt(),
                true,
                false
        );
        System.out.println(test);
        test.start();
    }

    private Queue<FinalAggregation> runTest(AbstractSlidingWindowAggregation<Tuple, SliceAggregation, FinalAggregation> swag) {
        Queue<FinalAggregation> output = new ArrayDeque<>();
        if (isCountCased)
            swag.computeCountBasedSWAG(stream, output);
        else
            swag.computeTimeBasedSWAG(stream, output);
        return output;
    }

    private boolean test(ArrayList<Queue<FinalAggregation>> outputStreams) {

        int outputStreamSize = outputStreams.get(0).size();

        for (Queue<FinalAggregation> queue : outputStreams)
            if (queue.size() != outputStreamSize) return false;

        FinalAggregation temp1, temp2;

        Queue<FinalAggregation> baseline = outputStreams.get(0); // baseline is recal

        boolean result = true;

        while (!baseline.isEmpty()) {
            temp1 = baseline.poll();
            for (int i = 1; i < outputStreams.size(); i++) {
                temp2 = outputStreams.get(i).poll();
                if (!temp1.equals(temp2)) {
                    System.out.println(getAlgo(i) + " is not correct, ");
                    System.out.println(temp2);
                    System.out.println("the correct output is " + temp1 + ", but the wrong output is " + temp2);
                    result = false;
                }
            }
        }

        return result;
    }

    private void setUpCountBasedTest(int range, int slide) {
        ArrayList<Queue<FinalAggregation>> outputStreams = new ArrayList<>();

        System.out.println("start count-based test, range: " + range + ", slide: " + slide);

        if (range / slide <= 2048) {
            System.out.println("start test of Recal, range=" + range + ", slide=" + slide);
            outputStreams.add(runTest(new Recal<>(range, slide, aggregation))); // recal is the baseline for test.
        }

        if (isSliceCreated) {
            System.out.println("start test of TS, range=" + range + ", slide=" + slide);
            outputStreams.add(runTest(new TwoStack2<>(range, slide, aggregation)));
            System.out.println("start test of SD, range=" + range + ", slide=" + slide);
            outputStreams.add(runTest(new SlickDequeNonInv2<>(range, slide, aggregation)));
        } else {
            System.out.println("start test of TS, range=" + range + ", slide=" + slide);
            outputStreams.add(runTest(new TwoStack<>(range, slide, aggregation)));
            System.out.println("start test of SD, range=" + range + ", slide=" + slide);
            outputStreams.add(runTest(new SlickDequeNonInv<>(range, slide, aggregation)));
        }

        System.out.println("start test of SBA, range=" + range + ", slide=" + slide);
        outputStreams.add(runTest(new SequentialBoundaryAggregator<>(range, slide, aggregation)));

        System.out.println("start test of PBA, range=" + range + ", slide=" + slide);
        ParallelBoundaryAggregator.threadPool = Executors.newCachedThreadPool();
        outputStreams.add(runTest(new ParallelBoundaryAggregator<>(range, slide, aggregation)));
        ParallelBoundaryAggregator.threadPool.shutdown();

        System.out.println("test result: " + test(outputStreams) + ", range: " + range + ", slide: " + slide);

        outputStreams = null;
        System.gc();
    }

    public void start() {
        if (isCountCased)
            countBasedTest();
        else
            timeBasedTest();
    }

    private void countBasedTest() {
        int[] ranges = new int[20 - 3 + 1];
        ranges[0] = 2 * 2 * 2;
        for (int i = 1, length = ranges.length; i < length; i++)
            ranges[i] = ranges[i - 1] * 2;
        for (int range : ranges) // Scenario 1 with slice size of 1, range sizes from 2^3 to 2^{20}
            setUpCountBasedTest(range, 1);


        int[] slides = new int[10];
        slides[0] = 2;
        for (int i = 1, length = slides.length; i < length; i++)
            slides[i] = slides[i - 1] * 2;
        for (int slide : slides) // Scenario 2 with range size of 2^17, slide sizes from 2^1 to 2^3
            setUpCountBasedTest((int) Math.pow(2, 17), slide);
    }

    private void setUpTimeBasedTest(Duration range, Duration slide, boolean isSliceCreated) {
        ArrayList<Queue<FinalAggregation>> outputStreams = new ArrayList<>();

        System.out.println("start time-based test, range:" + range + ", slide:" + slide);

        if (range.getSeconds() / slide.getSeconds() <= 4096) {
            System.out.println("start test of Recal");
            outputStreams.add(runTest(new Recal<>(range, slide, aggregation))); // recal is the baseline for test.
        }

        if (isSliceCreated) {
            System.out.println("start test of TS");
            outputStreams.add(runTest(new TwoStack2<>(range, slide, aggregation)));
            System.out.println("start test of SD");
            outputStreams.add(runTest(new SlickDequeNonInv2<>(range, slide, aggregation)));
        } else {
            System.out.println("start test of TS");
            outputStreams.add(runTest(new TwoStack<>(range, slide, aggregation)));
            System.out.println("start test of SD");
            outputStreams.add(runTest(new SlickDequeNonInv<>(range, slide, aggregation)));
        }

        System.out.println("start test of SBA");
        outputStreams.add(runTest(new SequentialBoundaryAggregator<>(range, slide, aggregation)));

        System.out.println("start test of PBA");
        ParallelBoundaryAggregator.threadPool = Executors.newCachedThreadPool();
        outputStreams.add(runTest(new ParallelBoundaryAggregator<>(range, slide, aggregation)));
        ParallelBoundaryAggregator.threadPool.shutdown();


        System.out.println("test result: " + test(outputStreams) + ", range: " + range + ", slide: " + slide);

        outputStreams = null;
        System.gc();
    }

    private void timeBasedTest() {
        System.out.println("time-based test");
        Duration[] ranges = new Duration[6 - (-3) + 1];
        ranges[0] = Duration.ofSeconds(450); // 0.125 hour
        for (int i = 1, length = ranges.length; i < length; i++)// Scenario 1 with slice size of 1 second, ranges from 2^{-3} to 2^6 hour
            ranges[i] = Duration.ofSeconds(ranges[i - 1].getSeconds() * 2);
        for (Duration range : ranges)
            setUpTimeBasedTest(range, Duration.ofSeconds(1), isSliceCreated);

        Duration[] slides = new Duration[10];
        slides[0] = Duration.ofSeconds(2);
        for (int i = 1, length = slides.length; i < length; i++)// Scenario 2 with range size of 8 hours, slide from 2^1 to 2^{10} seconds.
            slides[i] = Duration.ofSeconds(slides[i - 1].getSeconds() * 2);
        for (Duration slide : slides)
            setUpTimeBasedTest(Duration.ofHours(8), slide, isSliceCreated);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Test{");
        sb.append("aggregation=").append(aggregation);
        sb.append(", isCountCased=").append(isCountCased);
        sb.append(", isSliceCreated=").append(isSliceCreated);
        sb.append('}');
        return sb.toString();
    }
}
