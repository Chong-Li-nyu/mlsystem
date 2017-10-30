var input = sc.textFile("/user/cl4056/mlsystem/wordcount/data/").flatMap(line => line.split(" "))

var result = input.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}

result.saveAsTextFile("/user/cl4056/mlsystem/wordcount/outputSpark")
//result.collect().foreach(println)
