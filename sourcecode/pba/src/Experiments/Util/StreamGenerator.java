package Experiments.Util;

import Tuples.IntTuple;
import Tuples.DoubleTuple;
import java.time.Duration;
import java.util.*;

public class StreamGenerator {
    public static Queue<IntTuple> generateStreamOfIntTuples (int num, boolean isRandom) {
        ArrayDeque<IntTuple> stream = new ArrayDeque<>(num);
        if (isRandom){
            Random random = new Random(System.nanoTime());
            for (int i=0; i<num; i++)
                stream.add(new IntTuple(random.nextInt(num)));
        } else
            for (int i=0; i<num; i++)
                stream.add(new IntTuple(num - i));
        return stream;
    }

    public static Queue<DoubleTuple> generateStreamOfDoubleTuplesWithTimeStamp (Duration duration, int ratePerSecond){
        ArrayDeque<DoubleTuple> stream = new ArrayDeque<>();

        long currentTime = System.currentTimeMillis();
        Random random = new Random(System.nanoTime());

        for (long i=0; i<duration.getSeconds(); i++){
            long tempStamp = currentTime + i * 1000;
            for (int j=0; j<ratePerSecond; j++){
                stream.add(new DoubleTuple(tempStamp, random.nextDouble() ));
            }
        }
        return stream;
    }
}
