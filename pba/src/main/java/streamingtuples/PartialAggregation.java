package streamingtuples;

public interface PartialAggregation<T> {
    /**
     * Update the content of the partial aggregation.
     *
     * @param t update the partial aggregation to be the partial aggregation t.
     */
    void update(T t);

    /**
     * Create an array of partial aggregation of type T.
     *
     * @param size the size of the array to be created.
     */
    T[] createPartialAggregationArray(int size);
}
