package Aggregations;

import Tuples.PartialAggregation;
import Tuples.StreamingTuple;

/**
 * @param <IN> type of streaming tuples.
 * @param <ACC> type of partial aggregations, or slice aggregations.
 * @param <OUT> type of final aggregation results.
 * The aggregate function interface follows the same signature as the one of Apache Flink (AggregateFunction)
 * Aggregate functions are usually defined by the LCL framework (lift, combine, Lower).
 * The lift and combine function together corresponds to the add function in this interface.
 * The combine function corresponds to the merge function in this interface.
 * The lower function corresponds to the getResult function in this interface.
 *
 * Different implementations of an aggregation will have a huge impact for system performance.
 * We discuss some lessons we learned from our experiments for each functions below.
 * The overall recommendation is to avoid creating instance on the fly.
 * Otherwise, throughput will be bounded by only several million events/second (one or two million events/second, depending on the size of main memory).
 * This is due to creating two many objects, which also leads to the overhead of GC.
 */
public interface Aggregation <IN extends StreamingTuple, ACC extends PartialAggregation<ACC>, OUT> {
    /**
     * This function will create a slice aggregation with an identity element, i.e., the 0 value for Sum, or the minimum value for Max.
     * In general, creatAccumulator() needs to create a new instance of accumulator with the zero value, e.g., Sum.
     * However, for some aggregations, e.g., Max and Min, a good implementation for creatAccumulator() is to return a constant objection with zero values, instead of creating an accumulator for each slice.
     * An example of such an implementation is shown by the MaxInts class under the same package.
     */
    ACC createAccumulator();

    /**
     * This function will merge two slice aggregations (a slice aggregation is a partial aggregation over a slice)
     * Please do not create a new accumulator instances in the implementation of merge().
     * For aggregations that will only return one of the two inputs, e.g., Max and Min, creating accumulators can be simply avoided.
     * For the others, e.g., Sum, updating one of the two inputs and return its references is a good choice.
     * Note that, the second case requires specific consideration of a SWAG algorithm. We provide the corresponding ones for TwoStack (TwoStackSliceCreation) and SlickDeque (SliceDequeSliceCreation).
     * BoundaryAggregators, i.e., SBA and PBA  naturally supports the above two cases.
     * @param write
     * @param read
     * @return
     */
    ACC merge(ACC write, ACC read);

    /**
     * This function will return the final output, which simply return the input of the function for Max, Min, and Sum.
     * For aggregations that has the same type of ACC and OUT, the object creation can be simply avoided.
     *
     * @param partialAggregation
     * @return
     */
    OUT getResult(ACC partialAggregation);


    /**
     * This function will accumulate streaming values inside each slices.
     * As add() is mathematically equivalent to the combination of lift and combine, please follows the implementation of createAccumulator() and merge().
     *
     * @param partialAggregation
     * @param input
     * @return
     */
    ACC add(ACC partialAggregation, IN input);

    /**
     * This function apply a merge function over an array of accumulators.
     * This function can void functional calls at runtime.
     * @param array an array of accumulators.
     */
    void mergeLCS(ACC[] array);
}
