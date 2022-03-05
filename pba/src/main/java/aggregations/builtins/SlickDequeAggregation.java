package aggregations.builtins;

import aggregations.Aggregation;
import swag.slickdeque.SlickDequeNonInv2;
import streamingtuples.PartialAggregation;
import streamingtuples.StreamingTuple;

import java.util.ArrayList;
import java.util.List;

public abstract class SlickDequeAggregation<IN extends StreamingTuple, ACC extends PartialAggregation<ACC>, OUT, ACCNI extends PartialAggregation<ACCNI>, OUTNI> implements Aggregation<IN, ACC, OUT> {

    ArrayList<Aggregation<IN, ACCNI, OUTNI>> listOfNonInvAgg;

    public SlickDequeAggregation() {
        this.listOfNonInvAgg = new ArrayList<>();
        initializeNonInvAgg();
    }

    public abstract ACC applyInverse(ACC write, ACC read);

    public void addNonInvAgg(Aggregation<IN, ACCNI, OUTNI> nonInvAgg) {
        listOfNonInvAgg.add(nonInvAgg);
    }

    public abstract void initializeNonInvAgg();

    public Aggregation<IN, ACCNI, OUTNI> getNonInvertibleAggregation(int index) {
        return listOfNonInvAgg.get(index);
    }

    public int getNumOfNonInvAgg() {
        return listOfNonInvAgg.size();
    }

    public abstract ACCNI getElementToInsert(ACC acc, int index);

    public abstract void setNonInvAggResultToACC(ACC acc, List<SlickDequeNonInv2<IN, ACCNI, OUTNI>> list);
}
