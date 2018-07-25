# Big Data for Health 

This repo contains some working scripts for ETL and machine learning on healthcare data using big data tools: Hadoop, Hive, Pig, Spark with Scala.


## ETL with Hive
* Write Hive code that computes various metrics on healthcare data 
* See `hive/`

## ETL with Pig 
* Write Pig code to convert the raw data to standardized format
* See `pig/`

## MapReduce in Hadoop 
* Implement SGD logistic regresssion from scratch in Python. 
* Write MapReduce program to train multiple logistic regression classifiers in parallel with Hadoop 
* See `Python`

## ETL using Spark with Scala
* Implement ETL to construct features out of the raw data. See `scala/features`
* Implement rule-based phenotyping. See `scala/phenotyping`
* Implement unsupervised phenotyping using k-means clustering, Gaussian Mixture Model, non-negative matrix factorization (NMF). See `scala/clustering`

## Grapical models to represent patient electronic healthcare record (EHR) data using Spark with Scala
* Constrauct graph models for patient EHR with Spark GraphX. See`scala_graph/graphcpnstruct`
* Implement random walk with retsart (RWR), a simple variation of PageRank, with Spark GraphX. See`scala_graph/randomwalk`
* Implement power iteration clustering (PIC), a scalable and efficient algorithm for clustering vertices of a graph given pairwise similarties as edge properties, with Spark MLlib. See`scala_graph/clustering`