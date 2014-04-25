Kmeans-hadoop-mapreduce
=======================

Implementation of the Kmeans algorithm for the Hadoop MapReduce framework.

Input to the mapreduce program should be matrix of computed TF-IDF values in the sparse representation eg.
doc1  car:0.9 plane:1.6 computer:2.3
doc2  elephant:0.2 hadoop:1.1
.
.
.
docn  mahout:0.8 storm:1.6

Output of this program will be in the following format:
x:doc1  car:0.9 plane:1.6 computer:2.3
y:doc2  elephant:0.2 hadoop:1.1
.
.
.
z:docn  mahout:0.8 storm:1.6

where x,y,z is from (0,k)

Usage:

1) You need to create jar:
use command: mvn clean package

2) Copy the jar to the machine where you have your Hadoop installed

3) Run program:
hadoop jar <name of the jar> com.zikesjan.bigdata.KmeansMain <number of clusters> <maximal number of iteration> <input path> <output path>

IMPORTANT: The code is not ment for production, it might work well in some cases, but it is not fully tested.
It is reccomended to use some standard implementation such as Apache Mahout instead of this code.
