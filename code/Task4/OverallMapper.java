import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OverallMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {


    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().toLowerCase();

            String[] positive_words = {"good", "great", "amazing", "awesome", "wonderful", "best"};

            for (String word : positive_words) {

                if (line.contains(word))
                    context.write(new Text("The Positive # of feedback for Self Driving Car is :"), new IntWritable(1));
            }

            String[] negative_words = {"bad", "worst", "not", "unlikely", "worse", "beware", "horrible"};

            for (String word : negative_words) {

                if (line.contains(word))
                    context.write(new Text("The Negative # of feedback for Self Driving Car is :"), new IntWritable(1));

            }
        
}
}