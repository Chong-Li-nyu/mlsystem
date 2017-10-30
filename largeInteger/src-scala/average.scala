var input = sc.textFile("/user/cl4056/mlsystem/largeInteger/input/data.txt").flatMap(line => line.split(" "));
var mapres = input.map(num => (1, num.toInt))
var redres = mapres.reduceByKey({case (x,y)=> x+y}).collect();
var result = redres(0)._2 /mapres.count
// access array from arr(0), tuple by tp._1
