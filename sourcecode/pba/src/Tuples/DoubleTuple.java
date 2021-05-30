package Tuples;

public class DoubleTuple extends StreamingTuple implements PartialAggregation<DoubleTuple> {
    private double value;

    public static final DoubleTuple MIN_VALUE = new DoubleTuple(Double.MIN_VALUE);
    public static final DoubleTuple MAX_VALUE = new DoubleTuple(Double.MAX_VALUE);
    public static final DoubleTuple ZERO = new DoubleTuple(0.0);

    public DoubleTuple(double v) {
        super(0);
        this.value = v;
    }

    public DoubleTuple(long timeStamp, double value) {
        super(timeStamp);
        this.value = value;
    }

    @Override
    public String toString() {
        return value + " ";
    }
    
    public double doubleValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public void update(DoubleTuple doubleTuple) {
        this.value = doubleTuple.value;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (!(obj instanceof DoubleTuple))
            return false;

        return this.value == ((DoubleTuple) obj).doubleValue();
    }


    @Override
    public DoubleTuple[] createArrayOfACC(int size) {
        return new DoubleTuple[size];
    }
}
