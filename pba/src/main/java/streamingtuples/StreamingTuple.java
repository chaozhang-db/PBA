package streamingtuples;

public class StreamingTuple {
    private final long timeStamp;

    public StreamingTuple(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public long getTimeStamp() {
        return timeStamp;
    }
}
