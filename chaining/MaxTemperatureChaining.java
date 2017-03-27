import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.conf;
public class MaxTemperatureChaining extends Configured{

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperature <input path> <output path>");
      System.exit(-1);
    }
    

   Configuration conf = new Configuration();
    JobConf job = new JobConf(conf,MaxTemperatureChaining.class);
    job.setJarByClass(MaxTemperatureChaining.class);
    job.setJobName("Max temperatureChaining");

    FileInputFormat.setInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));


    
     
JobConf map1Conf = new JobConf(false);

ChainMapper.addMapper(job,MaxTemperatureMapper6_1.class,LongWritable.class,Text.class,Text.class,IntWritable.class,true,map1Conf);




JobConf reduce1Conf = new JobConf(false);
ChainReducer.setReducer(job, MaxTemperatureReducer6_1.class, Text.class, IntWritable.class, Text.class, IntWritable.class, true, reduce1Conf);



JobConf map2Conf = new JobConf(false);

ChainMapper.addMapper(job,MaxTemperatureMapperC.class,Text.class,IntWritable.class,Text.class,IntWritable.class,true,map2Conf);


JobConf reduce2Conf = new JobConf(false);
ChainReducer.setReducer(job, MaxTemperatureReducerC.class, Text.class, IntWritable.class, Text.class, IntWritable.class, true, reduce2Conf);

JobClient.runJob(job);


    //job.setMapperClass(MaxTemperatureMapper6_1.class);
    //job.setReducerClass(MaxTemperatureReducer6_1.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
