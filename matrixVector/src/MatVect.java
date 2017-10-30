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

public class MatVect {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "largest num");
    job.setNumReduceTasks(1);//set reduce task one

    job.setJarByClass(MatVect.class);
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
    
    private static int n ; // dim of matrix 
    private static int[]  v = new int[100];
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      //first line is the vector, and following lines each is a row of matrix 
      n = Integer.parseInt(itr.nextToken());
      for (int i = 0; i< n; i++){
        v[i]= Integer.parseInt(itr.nextToken());
      }
      for (int i = 0; i< n; i++){
        for (int j = 0; j<n; j++){
          if ((itr.hasMoreTokens())){
            number.set( Integer.parseInt(itr.nextToken()) *v[j]  ) ;
            context.write(new IntWritable(i), number);  //share one key i, has different j, so plus them 
          }
        }
      }
        
    }
  }
 
  public static class SelfReducer
       extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private IntWritable sum_w = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      //tell if the key's value list size is only one
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      sum_w.set(sum) ;
      context.write( key, sum_w );
    }
  }

}
// javac -classpath `yarn classpath`:. -d . MatVect.java
// jar -cvf distinct.jar *.class
// hadoop jar  distinct.jar MatVect /user/cl4056/mlsystem/matrixVector/input/small.txt /user/cl4056/mlsystem/matrixVector/output
// hdfs dfs -cat /user/cl4056/mlsystem/matrixVector/output/*
// hdfs dfs -mkdir -p /user/cl4056/mlsystem/matrixVector/input
// hdfs dfs -put ./data/small.txt /user/cl4056/mlsystem/matrixVector/input/
