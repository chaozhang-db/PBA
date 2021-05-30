package SlidingWindowAggregations;

import Aggregations.Aggregation;
import SlidingWindowAggregations.BoundaryAggregator.*;
import SlidingWindowAggregations.SlickDeque.SlickDequeNonInv;
import SlidingWindowAggregations.SlickDeque.SlickDequeNonInvSliceCreation;
import SlidingWindowAggregations.TwoStack.TwoStack;
import SlidingWindowAggregations.TwoStack.TwoStackSliceCreation;
import Tuples.PartialAggregation;
import Tuples.StreamingTuple;

import java.time.Duration;

public class SlidingWindowAggregatorFactory<Tuple extends StreamingTuple, SliceAggregation extends PartialAggregation<SliceAggregation>, FinalAggregation> {
    public AbstractSlidingWindowAggregation<Tuple, SliceAggregation, FinalAggregation> getSWAG (String algorithm, int range, int slide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation){
        if (algorithm == null)
            return null;

        if (algorithm.equalsIgnoreCase("RECAL"))
            return new Recal<>(range,slide,aggregation);

        else if (algorithm.equalsIgnoreCase("SLICKDEQUE-NONINV"))
            return new SlickDequeNonInv<>(range,slide,aggregation);

        else if (algorithm.equalsIgnoreCase("TWOSTACK"))
            return new TwoStack<>(range,slide,aggregation);

        else if (algorithm.equalsIgnoreCase("SBA"))
            return new SequentialBoundaryAggregator<>(range,slide,aggregation);

        else if (algorithm.equalsIgnoreCase("PBA"))
            return new ParallelBoundaryAggregator<>(range,slide,aggregation);

        else if (algorithm.equalsIgnoreCase("PBA2"))
            return new ParallelBoundaryAggregator2<>(range,slide,aggregation);

        return null;
    }

    public AbstractSlidingWindowAggregation<Tuple, SliceAggregation, FinalAggregation> getSWAG (String algorithm, Duration range, Duration slide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation){
        if (algorithm == null)
            return null;

        if (algorithm.equalsIgnoreCase("RECAL"))
            return new Recal<>(range,slide,aggregation);

        else if (algorithm.equalsIgnoreCase("SLICKDEQUE-NONINV"))
            return new SlickDequeNonInv<>(range,slide,aggregation);

        else if (algorithm.equalsIgnoreCase("TWOSTACK"))
            return new TwoStack<>(range,slide,aggregation);

        else if (algorithm.equalsIgnoreCase("SBA"))
            return new SequentialBoundaryAggregator<>(range,slide,aggregation);

        else if (algorithm.equalsIgnoreCase("PBA"))
            return new ParallelBoundaryAggregator<>(range,slide,aggregation);

        else if (algorithm.equalsIgnoreCase("PBA2"))
            return new ParallelBoundaryAggregator2<>(range,slide,aggregation);

        return null;
    }

    public AbstractSlidingWindowAggregation<Tuple, SliceAggregation, FinalAggregation> getSWAGSliceCreation (String algorithm, int range, int slide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation){
        if (algorithm == null)
            return null;

        if (algorithm.equalsIgnoreCase("RECAL"))
            return new Recal<>(range,slide,aggregation);

        else if (algorithm.equalsIgnoreCase("SLICKDEQUE-NONIVN"))
            return new SlickDequeNonInvSliceCreation<>(range,slide,aggregation);

        else if (algorithm.equalsIgnoreCase("TWOSTACK"))
            return new TwoStackSliceCreation<>(range,slide,aggregation);

        else if (algorithm.equalsIgnoreCase("SBA"))
            return new SequentialBoundaryAggregator<>(range,slide,aggregation);

        else if (algorithm.equalsIgnoreCase("PBA"))
            return new ParallelBoundaryAggregator<>(range,slide,aggregation);

        else if (algorithm.equalsIgnoreCase("PBA2"))
            return new ParallelBoundaryAggregator2<>(range,slide,aggregation);

        return null;
    }

    public AbstractSlidingWindowAggregation<Tuple, SliceAggregation, FinalAggregation> getSWAGSliceCreation (String algorithm, Duration range, Duration slide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation){
        if (algorithm == null)
            return null;

        if (algorithm.equalsIgnoreCase("RECAL"))
            return new Recal<>(range,slide,aggregation);

        else if (algorithm.equalsIgnoreCase("SLICKDEQUE-NONINV"))
            return new SlickDequeNonInvSliceCreation<>(range,slide,aggregation);

        else if (algorithm.equalsIgnoreCase("TWOSTACK"))
            return new TwoStackSliceCreation<>(range,slide,aggregation);

        else if (algorithm.equalsIgnoreCase("SBA"))
            return new SequentialBoundaryAggregator<>(range,slide,aggregation);

        else if (algorithm.equalsIgnoreCase("SBA2"))
            return new SequentialBoundaryAggregator<>(range,slide,aggregation);

        else if (algorithm.equalsIgnoreCase("PBA"))
            return new ParallelBoundaryAggregator<>(range,slide,aggregation);

        else if (algorithm.equalsIgnoreCase("PBA2"))
            return new ParallelBoundaryAggregator2<>(range,slide,aggregation);

        return null;
    }
}
