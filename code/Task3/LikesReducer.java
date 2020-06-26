import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LikesReducer
  extends Reducer<Text, Text, Text, Text> {
  
  @Override
  public void reduce(Text key, Iterable<Text> values,
      Context context)
      throws IOException, InterruptedException {
    
    int total = 0;
    for (Text value : values) {
      String cars = value.toString();
      total += Integer.parseInt(cars);
    }
    String str = String.format("%d", total);
    context.write(key, new Text(str));
  }
}