var input = sc.textFile("/user/cl4056/mlsystem/largeInteger/input/data.txt").flatMap(line => line.split(" "));
def returnLarger(x:Int , y:Int) = if(x>y) x else y;
var result = input.map(num => (1, num.toInt)).reduceByKey(returnLarger);

result.collect().foreach(println)
result.saveAsTextFile("/user/cl4056/mlsystem/largeInteger/outputSpark1")
