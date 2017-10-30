def mapfunction(person: String, friends: Array[String]):Array[(String, Int)] = {
  var res = new Array[(String, Int)](0);
  for (i <- 0 to (friends.length - 1)){
    val pair = (person+","+friends(i), 1);
    res = res ++ Array(pair);
  }
  return res;
}

val lines = sc.textFile("hdfs:/user/cl4056/mlsystem/recommend/data/soc-LiveJournal1Adj.txt")

def genKeyVal(line:String ):(String,Array[String]) = {
  val sp = line.split("\\t");
  var v:Array[String]=new Array[String](0);
  if (sp.length > 1){v = sp(1).split(",") ; }
  return (sp(0), v );
}

val tsv = lines.map(genKeyVal)
val target_users = Array("8941", "9020", "9021", "9993")

for (target_user<-target_users){
  val friends = tsv.filter(_._1==target_user).flatMap(_._2) // Array[String] target_user's friends Array(8938, 8939, 8942, 8945, 8946)
  val friendsarr = friends.collect
  val strangers = tsv.filter({case line => (!friendsarr.contains(line._1) && (line._1!=target_user))  } ) //RDD[(String, Array[String])]

  val input = strangers.map(record  => ( record._1, record._2++friendsarr))

  val mapres = input.flatMap(record=> mapfunction(record._1, record._2))
  val redres = mapres.reduceByKey(_+_).filter(_._2 > 1 )
  val mapres2 = redres.map(record=> (  (record._1).split(",")(0), 1) )
  val redres2 = mapres2.reduceByKey(_+_)

  println(s"user: $target_user")
  if(redres2.count<=10){
    redres2.sortBy(_._2, false).saveAsTextFile("/user/cl4056/mlsystem/recommend/output"+target_user);
  }else{
    sc.parallelize(redres2.sortBy(_._2, false).take(10)).saveAsTextFile("/user/cl4056/mlsystem/recommend/output"+target_user);
  }

}
