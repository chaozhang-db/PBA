package benchmark;

public class BenchmarkParam {
    private final int range;
    private final int logFreq;
    private final int forkID;
    private final boolean isRandom;
    private final String path;
    private final boolean isLocal;

    public BenchmarkParam(int range,int forkID, boolean isRandom, String path, boolean isLocal) {
        this.range = range;
        this.logFreq = 1_000_000/2;
        this.forkID = forkID;
        this.isRandom = isRandom;
        this.path = path;
        this.isLocal = isLocal;
    }

    public int getRange() {
        return range;
    }

    public int getLogFreq() {
        return logFreq;
    }

    public int getForkID() {
        return forkID;
    }

    public boolean isRandom() {
        return isRandom;
    }

    public String getPath() {
        return path;
    }

    public boolean isLocal() {
        return isLocal;
    }
}
