package Tuples;

public class IntTuple extends StreamingTuple implements PartialAggregation<IntTuple> {
    private int value;

    public static final IntTuple MIN_VALUE = new IntTuple(Integer.MIN_VALUE);
    public static final IntTuple MAX_VALUE = new IntTuple(Integer.MAX_VALUE);
    public static final IntTuple ZERO = new IntTuple(0);

    public IntTuple(int v) {
        super(0);
        value = v;
    }

    public IntTuple(long timeStamp, int value) {
        super(timeStamp);
        this.value = value;
    }

    @Override
    public String toString() {
        return "IntTuple{" +
                "timeStamp=" + getTimeStamp() +
                ", value=" + value +
                '}';
    }
    public String toCSVRecord(){
        return getTimeStamp() + "," + value;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (!(obj instanceof IntTuple))
            return false;

        return this.value == ((IntTuple) obj).intValue();
    }

    public int intValue() {
        return value;
    }


    public void setValue(int value) {
        this.value = value;
    }

    @Override
    public void update(IntTuple v) {value = v.intValue();}

    @Override
    public IntTuple[] createArrayOfACC(int size) {
        return new IntTuple[size];
    }
}

