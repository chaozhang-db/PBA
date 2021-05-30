package fr.uca.db;

import slidingWindowAggregator.PrimitiveParallelBoundaryAggregator;

import java.util.*;

public class ParallelBoundaryAggregatorTest {


    public static void main(String[] args) {
       int num = 20000000, range = 10000, slide = 1, repeat = 50;
        Integer[] streamArray = getStreamArray(num);
        Integer[] outputOfPBAArray = new Integer[num];
//        Arrays.fill(outputOfPBAArray,Integer.MIN_VALUE);

        try {
            Thread.sleep(10*1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("*******");

        throughputTest(num,range,slide,repeat,streamArray,outputOfPBAArray);

//       System.out.println("==========");
//        int[] streamArrayPrimitive = getStreamArrayPrimitive(num);
//        int[] outputOfPBAArrayPrimitive = new int[num];
//       throughputTest(num,range,slide,repeat,streamArrayPrimitive,outputOfPBAArrayPrimitive);
    }


    static void throughputTest(int num, int range, int slide, int repeat, Integer[] streamArray, Integer[] outputOfPBAArray){

//        try {
//            Thread.sleep(10 * 1000 );
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//
//        System.out.println("start");
//
//        try {
//            Thread.sleep(10 * 1000 );
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        long start, end;
        for(int i=0; i<repeat; i++){
            start = System.currentTimeMillis();

            windowAggregatorWithPBA(streamArray,outputOfPBAArray,range,slide);

            end = System.currentTimeMillis();

            System.out.println(end - start);
        }
    }

//    static void throughputTest(int num, int range, int slide, int repeat, int[] streamArray, int[] outputOfPBAArray){
//
//
////        try {
////            Thread.sleep(10 * 1000 );
////        } catch (InterruptedException e) {
////            e.printStackTrace();
////        }
////
////        System.out.println("start");
////
////        try {
////            Thread.sleep(10 * 1000 );
////        } catch (InterruptedException e) {
////            e.printStackTrace();
////        }
//
//        long start, end;
//        for(int i=0; i<repeat; i++){
//            start = System.currentTimeMillis();
//
//            windowAggregatorWithPBA(streamArray,outputOfPBAArray,range,slide);
//
//            end = System.currentTimeMillis();
//
//            System.out.println(end - start);
//        }
//    }

    static void windowAggregatorWithPBA(Integer[] stream, Integer[] outputOfPBA, int range, int slide){
        ParallelBoundaryAggregator pba = new ParallelBoundaryAggregator(range, slide);
        int i = 0;
        for (Integer v : stream){
            pba.insert(v);
            outputOfPBA[i] = pba.query();
        }
        pba.close();
    }
//    static void windowAggregatorWithPBA(int[] stream, int[] outputOfPBA, int range, int slide){
//        PrimitiveParallelBoundaryAggregator pba = new PrimitiveParallelBoundaryAggregator(range, slide);
//        int i = 0;
//        for (int v : stream){
//            pba.insert(v);
//            outputOfPBA[i] = pba.query();
//        }
//    }

    static void windowAggregatorWithPBA(List<Integer> stream, List<Integer> outputOfPBA, int range, int slide){
        ParallelBoundaryAggregator pba = new ParallelBoundaryAggregator(range, slide);
        for (Integer v : stream){
            pba.insert(v);
            outputOfPBA.add(pba.query());
        }
    }

    static Integer[] getStreamArray (int num){
        Random random = new Random();
        Integer[] stream = new Integer[num];
        for (int i=0; i<num; i++) {
            stream[i] = random.nextInt(num);
        }
        return stream;
    }
    static int[] getStreamArrayPrimitive (int num){
        Random random = new Random();
        int[] stream = new int[num];
        for (int i=0; i<num; i++) {
            stream[i] = random.nextInt(num);
        }
        return stream;
    }

    static List<Integer> getStreamList (int num){
        Random random = new Random();
        List<Integer> stream = new ArrayList<>(num);
        for (int i=0; i<num; i++) {
            int temp = random.nextInt(num);
            stream.add(temp);
        }
        return stream;
    }

    static void resultTest(int num, int range, int slide){
        List<Integer> stream = getStreamList(num);
        ArrayList<Integer> outputOfPBA = new ArrayList<>();
        ArrayList<Integer> outputOfRecal = new ArrayList<>();

        Recal.run(stream,outputOfRecal,range);

        windowAggregatorWithPBA(stream,outputOfPBA,range,slide);

        boolean flag = true;
        for(int j=0, len=outputOfPBA.size(); j<len; j++){
            flag = outputOfPBA.get(j).equals(outputOfRecal.get(j));
        }
        System.out.print(flag);
    }

}

class Recal {
    static void run (List<Integer> stream, List<Integer> ret, int range){
        boolean begin = false;
        ArrayDeque<Integer> array = new ArrayDeque<>(range);
        int i=0;
        Integer  temp;
        Integer identity = Integer.MIN_VALUE;

        for(Integer v : stream){
            if (!begin){
                if (i++==range - 2) {
                    begin = true;
//                    i=0;
                }
                array.addLast(v);
            } else {
                array.addLast(v);
                temp = identity;
                for (Integer x : array)
                    temp = temp >= x ? temp : x;
                ret.add(temp);
                array.pollFirst();
            }
        }
    }
}
