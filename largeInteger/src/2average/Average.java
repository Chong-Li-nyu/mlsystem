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

public class Average {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "average num");
    job.setNumReduceTasks(1);//set reduce task one

    job.setJarByClass(Average.class);
    job.setMapperClass(SelfMapper.class);
    // job.setCombinerClass(SelfReducer.class);
    job.setReducerClass(SelfReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class SelfMapper
       extends Mapper<Object, Text, IntWritable, Text>{

    private final static IntWritable one = new IntWritable(1);
    private Text ret = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      int len = 0, chunk_sum = 0 ;
      while (itr.hasMoreTokens()) {
        len ++ ;
        chunk_sum += Integer.parseInt(itr.nextToken()) ;
      }

      ret.set(new Integer(len).toString() + ":" + new Integer(chunk_sum).toString());
      context.write(one, ret);
    }
  }

  public static class SelfReducer
       extends Reducer<IntWritable,Text,IntWritable,Text> {
    private Text result = new Text();

    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int len = 0, sum = 0;
      for (Text val : values) {
        String[] sp = val.toString().split(":");
        len += Integer.parseInt(sp[0]);
        sum += Integer.parseInt(sp[1]);
      }
      Double res = new Double(sum/len);
      result.set(res.toString());
      context.write(key, result);
    }
  }

}

// javac -classpath `yarn classpath`:. -d . Average.java
// jar -cvf average.jar *.class
// hadoop jar  average.jar Average /user/cl4056/mlsystem/largeInteger/input/data.txt /user/cl4056/mlsystem/largeInteger/output2
// hdfs dfs -cat /user/cl4056/mlsystem/largeInteger/output2/*
