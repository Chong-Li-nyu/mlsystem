import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Unique {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "largest num");
    job.setNumReduceTasks(1);//set reduce task one

    job.setJarByClass(Unique.class);
    job.setMapperClass(SelfMapper.class);
    job.setCombinerClass(SelfReducer.class);
    job.setReducerClass(SelfReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class SelfMapper
       extends Mapper<Object, Text, IntWritable, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private IntWritable number = new IntWritable();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        number.set( Integer.parseInt(itr.nextToken()) );
        context.write(number, one);
      }
    }
  }

  public static class SelfReducer
       extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    public void reduce(IntWritable key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      //tell if the key's value list size is only one
      int size = 0;
      for (IntWritable val : values) {
        size ++ ;
      }
      if (size == 1){
        context.write(key, one);
      }
    }
  }

}

// javac -classpath `yarn classpath`:. -d . Unique.java
// jar -cvf largest.jar *.class
// hadoop jar  unique.jar Unique /user/cl4056/mlsystem/largeInteger/input/data.txt /user/cl4056/mlsystem/largeInteger/output1
// hdfs dfs -cat /user/cl4056/mlsystem/largeInteger/output1/*
