Kmeans-hadoop-mapreduce
=======================

Implementation of the Kmeans algorithm for the Hadoop MapReduce framework.
<hr>
Input to the mapreduce program should be <b>matrix of computed TF-IDF values</b> in the sparse representation eg.

doc1  car:0.9 plane:1.6 computer:2.3<br>
doc2  elephant:0.2 hadoop:1.1<br>
.<br>
.<br>
.<br>
docn  mahout:0.8 storm:1.6<br>
<hr>
Output of this program will be in the following format:

x:doc1  car:0.9 plane:1.6 computer:2.3<br>
y:doc2  elephant:0.2 hadoop:1.1<br>
.<br>
.<br>
.<br>
z:docn  mahout:0.8 storm:1.6<br>

where x,y,z is from (0,k)
<hr>
<b>Usage:</b>

<b>1)</b> You need to create jar:
use command: <i>mvn clean package</i>

<b>2)</b> Copy the jar to the machine where you have your Hadoop installed

<b>3)</b> Run program:
<i>hadoop jar [name of the jar] com.zikesjan.bigdata.KmeansMain [number of clusters] [maximal number of iteration] [input path] [output path]</i>

<hr>
<b>IMPORTANT:</b> The code is not ment for production, it might work well in some cases, but it is not fully tested.
It is reccomended to use some standard implementation such as Apache Mahout instead of this code.
