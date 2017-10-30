import java.io.*;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.net.URI;

public class WordCount {

  public static void main(String[] args) throws Exception {
    
    Configuration conf = new Configuration();
    conf.setBoolean("dfs.support.append", true);

    for (int i = 1; i <= 100; i+=10) {
      for (int j = 1; j <= 100; j+=10) {
        long startTime = System.currentTimeMillis();

        JobConf jobConf = new JobConf(conf, WordCount.class);

        Job job = new Job(conf, "word count");

        jobConf.setNumMapTasks(i);
        jobConf.setNumReduceTasks(j);

        job.setJarByClass(WordCount.class);
        
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "M" + i + "R" + j));

        job.waitForCompletion(true);

        long endTime   = System.currentTimeMillis();
        long totalTime = endTime - startTime;

        try {
            FileSystem fs = FileSystem.get(URI.create("hdfs:/user/cl4056/mlsystem/wordcount/output/MRresult.txt"), conf);
            OutputStream out = fs.append(new Path("hdfs:/user/cl4056/mlsystem/wordcount/output/MRresult.txt"));
            InputStream in = new ByteArrayInputStream((i + " " + j + " " + totalTime+ "\n").getBytes("UTF-8"));
            IOUtils.copyBytes(in, out, 4096, true);
        }
        catch (Exception e) {
            System.out.print(e.getMessage());
        }
        if (j==1){j--;}
      }
      if(i==1) {i--;}
    }
  }
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }   
    }   
  }
  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }   
      result.set(sum);
      context.write(key, result);
    }   
  }
}
