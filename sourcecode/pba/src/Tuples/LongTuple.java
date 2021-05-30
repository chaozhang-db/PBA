package Tuples;

public class LongTuple extends StreamingTuple implements PartialAggregation<LongTuple> {
    private long value;

    public static final LongTuple MIN_VALUE = new LongTuple(Long.MIN_VALUE);
    public static final LongTuple MAX_VALUE = new LongTuple(Long.MAX_VALUE);
    public static final LongTuple ZERO = new LongTuple(0L);

    public LongTuple(long value) {
        super(0);
        this.value = value;
    }

    public LongTuple(long timeStamp, long value) {
        super(timeStamp);
        this.value = value;
    }

    @Override
    public String toString() {
        return value + " ";
    }

    public long longValue() {
        return value;
    }

    @Override
    public void update(LongTuple longTuple) {
        this.value = longTuple.value;
    }


    @Override
    public LongTuple[] createArrayOfACC(int size) {
        return new LongTuple[size];
    }
}
