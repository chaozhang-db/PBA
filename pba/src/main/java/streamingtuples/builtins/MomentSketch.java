package streamingtuples.builtins;

import streamingtuples.PartialAggregation;

import java.util.Arrays;

public class MomentSketch implements PartialAggregation<MomentSketch> {
    private static final double THRESHOLD = 1E-6;
    private final int k;
    private double min;
    private double max;
    private int count;
    private double[] moments;
    private double[] logMoments;

    public MomentSketch(int k) {
        this.k = k;
        this.min = Integer.MAX_VALUE;
        this.max = Integer.MIN_VALUE;
        this.count = 0;
        this.moments = new double[k];
        this.logMoments = new double[k];
    }

    public int getK() {
        return k;
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public double[] getMoments() {
        return moments;
    }

    public void setMoments(double[] moments) {
        this.moments = moments;
    }

    public double[] getLogMoments() {
        return logMoments;
    }

    public void setLogMoments(double[] logMoments) {
        this.logMoments = logMoments;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MomentSketch{");
        sb.append("k=").append(k);
        sb.append(", min=").append(min);
        sb.append(", max=").append(max);
        sb.append(", count=").append(count);
        sb.append(", moments=").append(Arrays.toString(moments));
        sb.append(", logMoments=").append(Arrays.toString(logMoments));
        sb.append('}');
        return sb.toString();
    }

    @Override
    public void update(MomentSketch momentSketch) {
        this.min = momentSketch.min;
        this.max = momentSketch.max;
        this.count = momentSketch.count;
        for (int i = 0; i < k; i++) {
            this.moments[i] = momentSketch.moments[i];
            this.logMoments[i] = momentSketch.logMoments[i];
        }
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (!(obj instanceof MomentSketch))
            return false;

        MomentSketch other = (MomentSketch) obj;
        if (k != other.k)
            return false;
        if (max != other.max)
            return false;
        if (min != other.min)
            return false;
        if (count != other.count)
            return false;
        for (int i = 0; i < k; i++) {
            if (Math.abs(moments[i] - other.getMoments()[i]) > THRESHOLD)
                return false;
            if (Math.abs(logMoments[i] - other.getLogMoments()[i]) > THRESHOLD)
                return false;
        }

        return true;
    }


    @Override
    public MomentSketch[] createPartialAggregationArray(int size) {
        return new MomentSketch[size];
    }
}
