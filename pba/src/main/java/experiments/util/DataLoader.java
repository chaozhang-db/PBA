package experiments.util;

import streamingtuples.builtins.IntTuple;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayDeque;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class DataLoader {
    public static final int size = 8_980_411;
    public static final String DEBS_12 = "./DEBS12-2012-02-23.txt", SYNTHETIC_DATA = "./synthetic-dataset-without-timestamps.txt", POISSON_STREAM = "./poisson-stream.txt";

    // Load dataset with following format: timestamp,value
    public static ArrayDeque<IntTuple> load(String path, boolean withTimeStamp, int expectedNumOfTuples, int copy) {
        ArrayDeque<IntTuple> stream = new ArrayDeque<>(expectedNumOfTuples * copy);
        try {
            long start, end, offset = 0;
            long timeStamp;
            while (copy > 0) {
                Scanner scanner = new Scanner(new File(path));
                String line;
                String[] s;

                while (scanner.hasNext()) {
                    line = scanner.nextLine();
                    s = line.split(",");
                    timeStamp = withTimeStamp ? Long.parseLong(s[0]) + offset : 0;
                    stream.add(new IntTuple(timeStamp, Integer.parseInt((s[1]))));
                }

                if (withTimeStamp) {
                    start = stream.peekFirst().getTimeStamp(); // for loading multiple copies of dataset
                    end = stream.peekLast().getTimeStamp();
                    offset = end - start + 1;
                }

                System.gc();
                TimeUnit.SECONDS.sleep(5);
                copy--;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return stream;
    }

    public static ArrayDeque<IntTuple> loadSyntheticDataWithoutTimeStamps(String path, int num) {
        ArrayDeque<IntTuple> stream = new ArrayDeque<>(num);
        try {
            Scanner scanner = new Scanner(new File(path));
            while (scanner.hasNext())
                stream.add(new IntTuple(Integer.parseInt(scanner.nextLine())));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return stream;
    }
}
