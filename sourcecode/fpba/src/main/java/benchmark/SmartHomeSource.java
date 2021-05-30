package benchmark;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import java.io.File;
import java.util.Scanner;

public class SmartHomeSource extends RichParallelSourceFunction<Tuple2<Integer,Double>> {
    private final String path;
    private volatile boolean running = true;
    private final boolean isLocal;

    public SmartHomeSource(String path, boolean isLocal) {
        this.path = path;
        this.isLocal = isLocal;
    }

    @Override
    public void run(SourceContext<Tuple2<Integer,Double>> sourceContext) throws Exception {
        System.out.println("experiments with smart home data");
        int para = getRuntimeContext().getNumberOfParallelSubtasks();
        int factor = getRuntimeContext().getIndexOfThisSubtask();

        Scanner scanner = isLocal ? new Scanner(new File(path + para+"/" + "part-0000"+factor+".csv"))
                                    : new Scanner(new File(path));

        String[] s;

        if (isLocal){
            while (running && scanner.hasNext()){
                s = scanner.nextLine().split(",");
                sourceContext.collect(Tuple2.of(Integer.parseInt(s[0]) , Double.parseDouble(s[1])));
            }
        } else {
            while (running && scanner.hasNext()){
                s = scanner.nextLine().split(",");
                sourceContext.collect(Tuple2.of(Integer.parseInt(s[4]) + 14 * factor , Double.parseDouble(s[2])));
            }
        }
        running = false;
    }

    @Override
    public void cancel() {
        running = false;
    }
}
