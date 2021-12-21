package Tuples;

public interface PartialAggregation<T> {
    /**
     * Deep copy of t
     * @param t
     */
    void update(T t);

    T[] createArrayOfACC(int size);
}
