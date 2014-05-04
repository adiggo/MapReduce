package edu.jhu.cs420.xiaoping;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;

import java.io.*;

// implements triangle counting with map/reduce

public class TriangleCount
{
  public static class TriangleCountMap extends Mapper<Object, Text, Text, NullWritable >
  {
    private final static NullWritable nullw = NullWritable.get();

    public void map(Object key, Text value, Context context ) 
        throws IOException, InterruptedException
    {
      // Get line
      String line = value.toString();

      // Token line based on whitespace
      StringTokenizer tokenizer = new StringTokenizer(line);

      // Get the ID of the person
      long id = Long.parseLong ( tokenizer.nextToken()); 

      // Build an array of friends
      int length = tokenizer.countTokens();
      long[] friends = new long [ length ]; 

      for ( int i=0; i<length; i++ )
      {
        friends[i] = Long.parseLong ( tokenizer.nextToken());
      }

      // output sorted threesome relation
      for ( int i=0; i<length-1; i++ )
      {
        for ( int j=i+1; j<length; j++ )
        {
          Text outputkey;

          if ( id < friends[i] )
          {   
            outputkey = new Text ( Long.toString(friends[j]) + " " + Long.toString(id) + " " +  Long.toString(friends[i]));
          } 
          else
          {
            outputkey = new Text ( Long.toString(friends[j]) + " " + Long.toString(friends[i]) + " " +  Long.toString(id));
          }

          
          context.write( outputkey, nullw );


          if ( id < friends[j] )
          {   
            outputkey = new Text ( Long.toString(friends[i]) + " " + Long.toString(id) + " " +  Long.toString(friends[j]));
          } 
          else
          {
            outputkey = new Text ( Long.toString(friends[i]) + " " + Long.toString(friends[j]) + " " +  Long.toString(id));
          }
          context.write( outputkey, nullw );
        }
      }
    }
  }
  // define class as static
  public static class TriangleCountReduce extends Reducer<Text, NullWritable, Text, NullWritable> 
  {
    
    private NullWritable r = NullWritable.get();

    
    public void reduce(Text key, Iterable<NullWritable> values, Context context) 
        throws IOException, InterruptedException
    {
      int sum = 0;

      for ( NullWritable val : values )
      {
        sum += 1;
      }
      
      // same key exist, valid
      if ( sum >= 2 )
      {
        context.write(key,r);
      }
    }
  }

  public static void main(String[] args) throws Exception 
  {
    // no combinor class
    JobConf jobconf = new JobConf(new Configuration(), TriangleCount.class);
    Job job = Job.getInstance(jobconf);
    job.setJarByClass(TriangleCount.class);
    job.setMapperClass(TriangleCountMap.class);
    job.setReducerClass(TriangleCountReduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
