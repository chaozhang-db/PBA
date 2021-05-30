package Experiments.Util;


import Tuples.IntTuple;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.time.OffsetDateTime;
import java.util.ArrayDeque;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class DataLoader {
    public static final int size = 8_980_411;
    public static final String ubuntuPath = "./DEBS12-2012-02-23.txt"; // change the path to the dataset

    // Load dataset with following format:
    // timestamp,value
    public static ArrayDeque<IntTuple> load(String path, boolean withTimeStamp, int num, int copy){
        ArrayDeque<IntTuple> stream = new ArrayDeque<>(num * copy);
        try{
            long start, end, offset = 0;
            long temp;
            while (copy > 0){
                Scanner scanner = new Scanner(new File(path));
                String line;
                String[] s;

                while (scanner.hasNext()){
                    line = scanner.nextLine();
                    s = line.split(",");
                    temp = withTimeStamp ? Long.parseLong(s[0]) + offset : 0;
                    stream.add(new IntTuple(temp, Integer.parseInt((s[1]))));
                }

                if (withTimeStamp){
                    start = stream.peekFirst().getTimeStamp(); // for loading multiple copies of dataset
                    end = stream.peekLast().getTimeStamp();
                    offset = end - start + 1;
                }

                System.gc();
                TimeUnit.SECONDS.sleep(5);
                copy--;
            }
        } catch (Exception e){
            e.printStackTrace();
        }
        return stream;
    }
}
