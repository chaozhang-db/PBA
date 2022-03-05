package aggregations;

import streamingtuples.PartialAggregation;
import streamingtuples.StreamingTuple;

/**
 * @param <IN>  the type of streaming tuples.
 * @param <ACC> the type of partial aggregations, or slice aggregations.
 * @param <OUT> the type of final aggregation results.
 *              The aggregate function interface follows the same signature as the one of Apache Flink (AggregateFunction)
 *              Aggregate functions are usually defined by the LCL framework (lift, combine, Lower).
 *              The lift and combine function together corresponds to the add function in this interface.
 *              The combine function corresponds to the merge function in this interface.
 *              The lower function corresponds to the getResult function in this interface.
 *              <p>
 *              Different implementations of an aggregation will have a huge impact for system performance.
 *              We discuss some lessons we learned from our experiments for each functions below.
 *              The overall recommendation is to avoid creating instance on the fly.
 *              Otherwise, throughput will be bounded by only several million events/second (one or two million events/second, depending on the size of main memory).
 *              This is due to creating two many objects, which also leads to the overhead of GC.
 */
public interface Aggregation<IN extends StreamingTuple, ACC extends PartialAggregation<ACC>, OUT> {
    /**
     * This function will create a slice aggregation with a corresponding identity element, e.g., the zero value for Sum, or the minimum value for Max.
     * In general, createAccumulator() needs to create a new instance of accumulator with the zero value, e.g., Sum.
     * However, for some aggregations, e.g., Max and Min, an optimized implementation for createAccumulator() is to return a constant object, instead of creating an accumulator for each slice.
     * An example of such an implementation is shown by the MaxInt class under the same package.
     */
    ACC createAccumulator();

    /**
     * This function will merge two slice aggregations (a slice aggregation is a partial aggregation of a slice)
     * For aggregations that will only return one of the two inputs, e.g., Max and Min, creating an accumulator object can be simply avoided.
     *
     * @param write the partial aggregation that will be written to.
     * @param read  the partial aggregation that will be read-only.
     * @return a partial aggregation.
     */
    ACC merge(ACC write, ACC read);

    /**
     * This function will return the final output, e.g., the identity function for Max, Min, and Sum.
     * For aggregations that has the same type of ACC and OUT, the object creation can be avoided.
     *
     * @param partialAggregation a partial aggregation.
     * @return the final aggregation.
     */
    OUT getResult(ACC partialAggregation);

    /**
     * This function will accumulate streaming values inside each slice.
     * The function add() is equivalent to the function that is the combination of lift and combine.
     *
     * @param partialAggregation the partial aggregation of a slice.
     * @param input              a streaming tuple to be computed.
     * @return the partial aggregation.
     */
    ACC add(ACC partialAggregation, IN input);

    /**
     * This function apply a merge function over an array of accumulators.
     * This function can void functional calls at runtime.
     *
     * @param array an array of accumulators.
     */
    void computeLeftCumulativeSliceAggregation(ACC[] array);
}
