package Experiments;

import Aggregations.*;
import Experiments.Util.StreamGenerator;
import SlidingWindowAggregations.*;
import SlidingWindowAggregations.BoundaryAggregator.ParallelBoundaryAggregator;
import SlidingWindowAggregations.BoundaryAggregator.SequentialBoundaryAggregator;
import SlidingWindowAggregations.SlickDeque.SlickDequeSliceCreation;
import SlidingWindowAggregations.TwoStack.TwoStackSliceCreation;
import Tuples.DoubleTuple;
import Tuples.MomentSketch;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Queue;

public class TestWithMomentSketch {
    private Queue<DoubleTuple> stream;
    private int k;
    public TestWithMomentSketch(Queue<DoubleTuple> stream, int k) {
        this.stream = stream;
        this.k = k;
    }

    private boolean test(ArrayList<Queue<MomentSketch>> outputStreams) {

        int outputStreamSize = outputStreams.get(0).size();

        for (Queue<MomentSketch> queue : outputStreams)
            if (queue.size() != outputStreamSize) return false;

        MomentSketch temp1, temp2;

        Queue<MomentSketch> baseline = outputStreams.get(0); // baseline is recal

        boolean result = true;

        while (!baseline.isEmpty()) {
            temp1 = baseline.poll();
            for (int i = 1; i < outputStreams.size(); i++) {
                temp2 = outputStreams.get(i).poll();
                if (!temp1.equals(temp2)) {
                    System.out.println(Test.getAlgo(i) + " is not correct, ");
                    System.out.println("the correct output is ");
                    System.out.println(temp1);
                    System.out.println("the wrong output is");
                    System.out.println(temp2);
                    result = false;
                }
            }
        }

        return result;
    }

    private Queue<MomentSketch> runTest(AbstractSlidingWindowAggregation<DoubleTuple, MomentSketch, MomentSketch> swag, boolean isCountCased){
        Queue<MomentSketch> output = new ArrayDeque<>();
        if (isCountCased)
            swag.computeCountBasedSWAG(stream,output);
        else
            swag.computeTimeBasedSWAG(stream,output);
        return output;
    }

    private void setUpTimeBasedTest(Duration range, Duration slide) {
        ArrayList<Queue<MomentSketch>> outputStreams = new ArrayList<>();

        System.out.println("start time-based test, range:" + range + ", slide:" + slide);

        Aggregation<DoubleTuple, MomentSketch, MomentSketch> aggregation = new ComputeMomentSketch(k);

        if(range.getSeconds() / slide.getSeconds() <= 4096){
            System.out.println("start test of Recal");
            outputStreams.add(runTest(new Recal<>(range, slide, aggregation),false)); // recal is the baseline for test.
        }


        System.out.println("start test of TS");
        outputStreams.add(runTest(new TwoStackSliceCreation<>(range, slide, aggregation),false));

        System.out.println("start test of SD");
        outputStreams.add(runTest(new SlickDequeSliceCreation<>(range, slide, new SlickDequeMomentSketchAgg(k)),false));


        System.out.println("start test of SBA");
        outputStreams.add(runTest(new SequentialBoundaryAggregator<>(range, slide, aggregation),false));

        System.out.println("start test of PBA");
        outputStreams.add(runTest(new ParallelBoundaryAggregator<>(range, slide, aggregation),false));


        System.out.println("test result: " + test(outputStreams) + ", range: " + range + ", slide: " + slide);

        outputStreams = null;
        System.gc();
    }

    public void timeBasedTest() {
        System.out.println("time-based test");
        setUpTimeBasedTest(Duration.ofHours(4), Duration.ofSeconds(1));
    }

    public static void main(String[] args) {
        TestWithMomentSketch test = new TestWithMomentSketch(StreamGenerator.generateStreamOfDoubleTuplesWithTimeStamp(Duration.ofDays(1),10),10);
        test.timeBasedTest();
    }

}
