import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OverallDriver {

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: OverallDriver <overall input path> <overallsentiment input path> <output path>");
      System.exit(-1);
    }
    
    Job job = new Job();
    job.setJarByClass(OverallDriver.class);
    job.setJobName("Overall Driver");

    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, OverallMapper.class);
    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, OverallSentimentMapper.class);
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    
    job.setReducerClass(OverallReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
// ^^ MinTemperature