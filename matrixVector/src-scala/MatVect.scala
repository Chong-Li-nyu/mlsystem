var input = sc.textFile("/user/cl4056/mlsystem/matrixVector/input/small.txt").flatMap(line => line.split(" "));
var n = (input.collect())(0).toInt
var vec = input.collect().slice(1, 1+n).map(x=>x.toInt)
var mat =input.collect().slice(1+n,1+n+n*n).map(x=>x.toInt)

val keyval = mat.zipWithIndex.map{case (s,i)=> (i/n, vec(i%n)*s)}
val result = sc.parallelize(keyval).reduceByKey(_+_).sortByKey(true)
result.collect.foreach(println)
result.saveAsTextFile("/user/cl4056/mlsystem/matrixVector/outputSpark1")
