# Boundary Aggregators
The repository provides two boundary aggregators for computing aggregations over sliding windows, namely sequential and parallel boundary aggregators (SBA and PBA).

#### Orginaztion 
[pba](https://github.com/chaozhang-db/PBA/tree/main/pba) includes an implementation of [SBA](https://github.com/chaozhang-db/PBA/blob/main/pba/src/main/java/swag/boundaryaggregator/SequentialBoundaryAggregator.java) and [PBA](https://github.com/chaozhang-db/PBA/blob/main/pba/src/main/java/swag/boundaryaggregator/ParallelBoundaryAggregator.java), as well as a standalone performance evaluation framework.

[fpba](https://github.com/chaozhang-db/PBA/tree/main/fpba) shows a simple integration of PBA on top of Apache Flink.