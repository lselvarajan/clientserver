//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.mapred.lib.ChainMapper;
//import org.apache.hadoop.mapred.lib.ChainReducer;
//import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.chain.*;
//import org.apache.hadoop.mapreduce.lib.chain.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.conf;
public class MaxTemperatureChaining {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperature <input path> <output path>");
      System.exit(-1);
    }
    
  
   Configuration conf = new Configuration();
	Job job=Job.getInstance();
    //JobConf job = new JobConf(conf,MaxTemperatureChaining.class);
    job.setJarByClass(MaxTemperatureChaining.class);
    job.setJobName("MaxtemperatureChaining");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));


    
     
Configuration map1Conf = new Configuration();

ChainMapper.addMapper(job,MaxTemperatureMapper6_1.class,LongWritable.class,Text.class,Text.class,IntWritable.class,map1Conf);




Configuration reduce1Conf = new Configuration();
ChainReducer.setReducer(job, MaxTemperatureReducer6_1.class, Text.class, IntWritable.class, Text.class, IntWritable.class, reduce1Conf);



Configuration map2Conf = new Configuration();

ChainMapper.addMapper(job,MaxTemperatureMapperC.class,Text.class,IntWritable.class,Text.class,IntWritable.class,map2Conf);


//Configuration reduce2Conf = new Configuration();
//ChainReducer.setReducer(job, MaxTemperatureReducerC.class, Text.class, IntWritable.class, Text.class, IntWritable.class,reduce2Conf);

//JobClient.runJob(job);

 
        job.setReducerClass(MaxTemperatureReducerC.class);
    //job.setMapperClass(MaxTemperatureMapper6_1.class);
    //job.setReducerClass(MaxTemperatureReducer6_1.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
