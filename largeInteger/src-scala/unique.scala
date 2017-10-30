var input = sc.textFile("/user/cl4056/mlsystem/largeInteger/input/datasmall.txt").flatMap(line => line.split(" "));
var mapres = input.map(num => (num.toInt, 1));
var redres = mapres.reduceByKey({case (x,y)=>x+y}).collect().filter({case x=>x._2==1});
var result = redres.map({case x=> x._1})
// access array from arr(0), tuple by tp._1
