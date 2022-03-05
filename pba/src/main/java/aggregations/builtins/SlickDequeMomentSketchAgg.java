package aggregations.builtins;

import swag.slickdeque.SlickDequeNonInv2;
import streamingtuples.builtins.DoubleTuple;
import streamingtuples.builtins.MomentSketch;

import java.util.List;

public class SlickDequeMomentSketchAgg extends SlickDequeAggregation<DoubleTuple, MomentSketch, MomentSketch, DoubleTuple, DoubleTuple> {
    private final int k;

    public SlickDequeMomentSketchAgg(int k) {
        super();
        this.k = k;
    }

    @Override
    public MomentSketch createAccumulator() {
        return new MomentSketch(k);
    }

    @Override
    public MomentSketch merge(MomentSketch write, MomentSketch read) {
        write.setCount(write.getCount() + read.getCount());
        for (int i = 0; i < k; i++) {
            write.getMoments()[i] += read.getMoments()[i];
            write.getLogMoments()[i] += read.getLogMoments()[i];
        }
        return write;
    }

    @Override
    public MomentSketch getResult(MomentSketch partialAggregation) {
        return partialAggregation;
    }

    @Override
    public MomentSketch add(MomentSketch partialAggregation, DoubleTuple input) {
        partialAggregation.setMin(Math.min(partialAggregation.getMin(), input.getValue()));
        partialAggregation.setMax(Math.max(partialAggregation.getMax(), input.getValue()));
        partialAggregation.setCount(partialAggregation.getCount() + 1);

        double v = input.getValue();
        double tempV = v;

        double logV = Math.log(v);
        double tempLogV = logV;

        double[] moments = partialAggregation.getMoments();
        double[] logMoments = partialAggregation.getLogMoments();
        for (int i = 0; i < k; i++) {
            moments[i] += tempV;
            logMoments[i] += tempLogV;

            tempV *= v;
            tempLogV *= logV;
        }

        return partialAggregation;
    }

    @Override
    public void computeLeftCumulativeSliceAggregation(MomentSketch[] array) {
    }

    @Override
    public MomentSketch applyInverse(MomentSketch write, MomentSketch read) {
        write.setCount(write.getCount() - read.getCount());
        for (int i = 0; i < k; i++) {
            write.getMoments()[i] -= read.getMoments()[i];
            write.getLogMoments()[i] -= read.getLogMoments()[i];
        }
        return write;
    }

    @Override
    public void initializeNonInvAgg() {
        addNonInvAgg(new MinDouble2()); // the fist NonInvAgg is Min
        addNonInvAgg(new MaxDouble2()); // the second NonInvAgg is Max
    }

    @Override
    public DoubleTuple getElementToInsert(MomentSketch momentSketch, int index) {
        DoubleTuple ret;
        if (index == 0) {
            ret = new DoubleTuple(momentSketch.getMin());
        } else {
            ret = new DoubleTuple(momentSketch.getMax());
        }
        return ret;
    }

    @Override
    public void setNonInvAggResultToACC(MomentSketch momentSketch, List<SlickDequeNonInv2<DoubleTuple, DoubleTuple, DoubleTuple>> list) {
        momentSketch.setMin(list.get(0).query().getValue());
        momentSketch.setMax(list.get(1).query().getValue());
    }
}
