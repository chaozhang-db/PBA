package swag;

import aggregations.Aggregation;
import swag.boundaryaggregator.ParallelBoundaryAggregator2;
import swag.boundaryaggregator.ParallelBoundaryAggregator;
import swag.boundaryaggregator.SequentialBoundaryAggregator;
import swag.slickdeque.SlickDequeNonInv;
import swag.slickdeque.SlickDequeNonInv2;
import swag.twostack.TwoStack2;
import swag.twostack.TwoStack;
import streamingtuples.PartialAggregation;
import streamingtuples.StreamingTuple;

import java.time.Duration;

public class SlidingWindowAggregatorFactory<Tuple extends StreamingTuple, SliceAggregation extends PartialAggregation<SliceAggregation>, FinalAggregation> {
    public AbstractSlidingWindowAggregation<Tuple, SliceAggregation, FinalAggregation> getSWAG(String algorithm, int range, int slide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        if (algorithm == null)
            return null;

        if (algorithm.equalsIgnoreCase("RECAL"))
            return new Recal<>(range, slide, aggregation);

        else if (algorithm.equalsIgnoreCase("SLICKDEQUE-NONINV"))
            return new SlickDequeNonInv<>(range, slide, aggregation);

        else if (algorithm.equalsIgnoreCase("TWOSTACK"))
            return new TwoStack<>(range, slide, aggregation);

        else if (algorithm.equalsIgnoreCase("SBA"))
            return new SequentialBoundaryAggregator<>(range, slide, aggregation);

        else if (algorithm.equalsIgnoreCase("PBA"))
            return new ParallelBoundaryAggregator<>(range, slide, aggregation);

        else if (algorithm.equalsIgnoreCase("PBA2"))
            return new ParallelBoundaryAggregator2<>(range, slide, aggregation);

        return null;
    }

    public AbstractSlidingWindowAggregation<Tuple, SliceAggregation, FinalAggregation> getSWAG(String algorithm, Duration range, Duration slide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        if (algorithm == null)
            return null;

        if (algorithm.equalsIgnoreCase("RECAL"))
            return new Recal<>(range, slide, aggregation);

        else if (algorithm.equalsIgnoreCase("SLICKDEQUE-NONINV"))
            return new SlickDequeNonInv<>(range, slide, aggregation);

        else if (algorithm.equalsIgnoreCase("TWOSTACK"))
            return new TwoStack<>(range, slide, aggregation);

        else if (algorithm.equalsIgnoreCase("SBA"))
            return new SequentialBoundaryAggregator<>(range, slide, aggregation);

        else if (algorithm.equalsIgnoreCase("PBA"))
            return new ParallelBoundaryAggregator<>(range, slide, aggregation);

        else if (algorithm.equalsIgnoreCase("PBA2"))
            return new ParallelBoundaryAggregator2<>(range, slide, aggregation);

        return null;
    }

    public AbstractSlidingWindowAggregation<Tuple, SliceAggregation, FinalAggregation> getSWAGSliceCreation(String algorithm, Duration range, Duration slide, Aggregation<Tuple, SliceAggregation, FinalAggregation> aggregation) {
        if (algorithm == null)
            return null;

        if (algorithm.equalsIgnoreCase("RECAL"))
            return new Recal<>(range, slide, aggregation);

        else if (algorithm.equalsIgnoreCase("SLICKDEQUE-NONINV"))
            return new SlickDequeNonInv2<>(range, slide, aggregation);

        else if (algorithm.equalsIgnoreCase("TWOSTACK"))
            return new TwoStack2<>(range, slide, aggregation);

        else if (algorithm.equalsIgnoreCase("SBA"))
            return new SequentialBoundaryAggregator<>(range, slide, aggregation);

        else if (algorithm.equalsIgnoreCase("PBA"))
            return new ParallelBoundaryAggregator<>(range, slide, aggregation);

        else if (algorithm.equalsIgnoreCase("PBA2"))
            return new ParallelBoundaryAggregator2<>(range, slide, aggregation);

        return null;
    }
}
