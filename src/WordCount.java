//package mlsystem.src;

import java.io.File; 
import java.util.*; 
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;


public class WordCount{
  public static void main(String[] args){
      //mlsystem.src.ReadFilesIndir ins = new mlsystem.src.ReadFilesIndir();
       ReadFilesInDir ins = new ReadFilesInDir();
       List<File> datafiles = ins.listDataFiles();
       for (File fileEntry : datafiles){
           System.out.println(fileEntry.getName());
       }

  }
  
}