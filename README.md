# Boundary Aggregators
The repository provides two boundary aggregators for computing aggregations over sliding windows, namely sequential and parallel boundary aggregators (SBA and PBA).

#### Orginaztion 
[pba] includes an implementation of SBA(https://github.com/chaozhang-db/PBA/blob/main/pba/src/main/java/swag/boundaryaggregator/SequentialBoundaryAggregator.java) and PBA(https://github.com/chaozhang-db/PBA/blob/main/pba/src/main/java/swag/boundaryaggregator/ParallelBoundaryAggregator.java), as well as a standalone performance evaluation framework.

[fpba] shows a simple integration of pba on top of Apache Flink.