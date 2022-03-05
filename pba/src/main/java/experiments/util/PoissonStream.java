package experiments.util;

import org.apache.commons.math3.distribution.PoissonDistribution;
import streamingtuples.builtins.IntTuple;

import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Random;

public class PoissonStream {

    private static final int LAMBDA = 5, MAX_ELEMENT = 10_000;
    private final long durationInMilli;
    private final PoissonDistribution poissonDistribution;

    public PoissonStream(Duration duration) {
        this.durationInMilli = duration.toMillis();
        poissonDistribution = new PoissonDistribution(LAMBDA);
    }

    public static void main(String[] args) {
        Queue<IntTuple> queue = new PoissonStream(Duration.ofDays(Integer.parseInt(args[0]))).generate();
        try {
            PrintWriter printWriter = new PrintWriter(new FileOutputStream(("./poisson-stream.txt")));
            for (IntTuple intTuple : queue) {
                printWriter.append(String.valueOf(intTuple.getTimeStamp())).append(",").append(String.valueOf(intTuple.intValue()));
                printWriter.append("\n");
                printWriter.flush();
            }
            printWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Generate a stream with timestamps, where the distribution of events within every 10 milliseconds follows the Poisson Distribution.
    // The generation starts with a timestamp of 0, representing the current time
    public Queue<IntTuple> generate() {
        ArrayDeque<IntTuple> arrayDeque = new ArrayDeque<>();
        Random random = new Random();
        for (long i = 0; i < durationInMilli; i++) {
            if (i % 10 != 0)
                continue;
            int sample = poissonDistribution.sample();
            if (sample == 0)
                continue;
            for (int j = 0; j < sample; j++) {
                arrayDeque.add(new IntTuple(i, random.nextInt(MAX_ELEMENT)));
            }
        }
        return arrayDeque;
    }
}
