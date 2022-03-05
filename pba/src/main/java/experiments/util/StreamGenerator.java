package experiments.util;

import streamingtuples.builtins.DoubleTuple;
import streamingtuples.builtins.IntTuple;

import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Random;

public class StreamGenerator {
    public static void main(String[] args) {
        Queue<IntTuple> queue = generateStreamOfIntTuples(200_000_000, true);
        try {
            PrintWriter printWriter = new PrintWriter(new FileOutputStream(("./synthetic-dataset-without-timestamps.txt")));
            for (IntTuple intTuple : queue) {
                printWriter.append(String.valueOf(intTuple.intValue()));
                printWriter.append("\n");
                printWriter.flush();
            }
            printWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Queue<IntTuple> generateStreamOfIntTuples(int num, boolean isRandom) {
        ArrayDeque<IntTuple> stream = new ArrayDeque<>(num);
        if (isRandom) {
            Random random = new Random(System.nanoTime());
            for (int i = 0; i < num; i++)
                stream.add(new IntTuple(random.nextInt(10_000)));
        } else
            for (int i = 0; i < num; i++)
                stream.add(new IntTuple(num - i));
        return stream;
    }
    public static IntTuple[] generateStreamOfIntTuples2(int num, boolean isRandom) {
        IntTuple[] stream = new IntTuple[num];
        if (isRandom) {
            Random random = new Random(System.nanoTime());
            for (int i = 0; i < num; i++)
                stream[i] = new IntTuple(random.nextInt(num));
        } else
            for (int i = 0; i < num; i++)
                stream[i] = new IntTuple(num - i);
        return stream;
    }

    public static Queue<DoubleTuple> generateStreamOfDoubleTuplesWithTimeStamp(Duration duration, int ratePerSecond) {
        ArrayDeque<DoubleTuple> stream = new ArrayDeque<>();

        long currentTime = System.currentTimeMillis();
        Random random = new Random(System.nanoTime());

        for (long i = 0; i < duration.getSeconds(); i++) {
            long tempStamp = currentTime + i * 1000;
            for (int j = 0; j < ratePerSecond; j++) {
                stream.add(new DoubleTuple(tempStamp, random.nextDouble()));
            }
        }
        return stream;
    }
}
