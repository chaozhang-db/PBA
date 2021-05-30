package benchmark;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class BenchmarkRunner {
    public static String localPathToSmartHomeDataset = ""; // path to data set

    public static String clusterPathToSmartHomeDataset = "";


    static int[] ranges = new int[]{1024};
    static int iteration = 10;

    public static void main(String[] args) throws Exception {
//        clusterExecute();
        localExecute();
    }

    static void clusterExecute() throws Exception{
        for (int range: ranges){
            for (int j=0; j<2; j++){
                int repeat = 0;
                while (repeat < iteration){
                    String path = clusterPathToSmartHomeDataset;
                    BenchmarkParam ben = new BenchmarkParam(range, repeat, false, path,false);
                    execute(6*4, ben,j);
                    repeat ++;
                }
                Thread.sleep(10_000);
                System.gc();
            }
        }
    }

    static void localExecute() throws Exception{
        for (int range: ranges){
            for (int j=0; j<2; j++){
                int repeat = 0;
                while (repeat < iteration){
                    for (int i=1; i<=Runtime.getRuntime().availableProcessors(); i++){
                        String path = localPathToSmartHomeDataset;
                        BenchmarkParam ben = new BenchmarkParam(range,repeat, false, path,true);
                        execute(i, ben, j);
                    }
                    repeat ++;
                }
                System.gc();
                Thread.sleep(10_000);
            }
        }
    }

    static void execute(int parallelism, BenchmarkParam ben, int method) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        if (method == 0)
            new ParallelBoundaryAggregatorBenchmarkJob(env,ben);
        else
            new FlinkBenchmarkJob(env,ben);

        System.gc();
        Thread.sleep(5_000);
    }
}