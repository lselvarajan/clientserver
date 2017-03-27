import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
public class MaxTemperatureMapperC
  extends Mapper<Text, IntWritable, Text, IntWritable> {

  private static final int MISSING = 9999;

  @Override
  public void map(Text key, IntWritable value, Context context)
      throws IOException, InterruptedException {
            String line = key.toString();
            String newline=line.replaceAll("\\s","");
            String SID=newline.substring(0,12);
            // String year=newline.substring(12,16);
            String day_month=newline.substring(16,20);
           // String airtempS=newline.substring(20);
            int airtemp=value.get();


            String newkey=SID+" "+day_month;
            context.write(new Text(newkey), new IntWritable(airtemp));
}
}
