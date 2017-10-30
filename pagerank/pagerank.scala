import scala.collection.mutable.HashMap

val links = sc.textFile("hdfs:/user/cl4056/mlsystem/pagerank/data/graph.txt").map(line => (line.split("""\t""")(0).toInt, line.split("""\t""")(1).toInt))

var ranks = sc.textFile("hdfs:/user/cl4056/mlsystem/pagerank/data/graph.txt").map(line => (line.split("""\t""")(0).toInt, 1.toDouble)).groupByKey().map(x => (x._1, 1.toDouble))

val outgoing = new HashMap[Int, Int]()

sc.textFile("hdfs:/user/cl4056/mlsystem/pagerank/data/graph.txt").map(line => (line.split("""\t""")(0).toInt, 1)).groupByKey().collect.foreach(x => outgoing(x._1) = x._2.sum)

for (i <- 1 to 10) {
  val dist = links.join(ranks).map(x => (x._2._1, if (outgoing.contains(x._1.toInt)) x._2._2.toDouble / outgoing(x._1.toInt) else 0))
  ranks = dist.reduceByKey(_ + _).mapValues(0.2 + 0.8 * _)
}

ranks.sortBy(_._1).map(x => x._1 + "\t" + x._2).saveAsTextFile("hdfs:/user/cl4056/mlsystem/pagerank/output")
