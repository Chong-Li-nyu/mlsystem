import java.io.*;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DistinctCount {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "largest num");
    job.setNumReduceTasks(1);//set reduce task one

    job.setJarByClass(DistinctCount.class);
    job.setMapperClass(SelfMapper.class);
    // job.setCombinerClass(SelfReducer.class);
    job.setReducerClass(SelfReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class SelfMapper extends Mapper<Object, Text, IntWritable, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private IntWritable number = new IntWritable();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        number.set( Integer.parseInt(itr.nextToken()) );
        context.write(one, number); //(1, 9999), put vals in a hashset
      }
    }
  }
 
  public static class SelfReducer
       extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private IntWritable count_w = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      //tell if the key's value list size is only one
      int len = 0;
      Set<Integer> distinct_set = new HashSet<Integer>();
      for (IntWritable val : values) {
        distinct_set.add(val.get()) ;
      }
      int count = distinct_set.size();
      count_w.set(count) ;
      context.write( count_w, one);
    }
  }

}

// javac -classpath `yarn classpath`:. -d . DistinctCount.java
// jar -cvf distinct.jar *.class
// hadoop jar  distinct.jar DistinctCount /user/cl4056/mlsystem/largeInteger/output3/part-r-00000 /user/cl4056/mlsystem/largeInteger/output4
// hdfs dfs -cat /user/cl4056/mlsystem/largeInteger/output4/*
