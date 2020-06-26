import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OverallSentimentMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {


    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().toLowerCase();
        String audi = "audi";
        if(line.contains(audi)) {
            String[] positive_words = {"good", "great", "amazing", "awesome", "wonderful", "best"};

            for (String word : positive_words) {

                if (line.contains(word))
                    context.write(new Text("The Positive # of feedback for Audi is :"), new IntWritable(1));

            }

            String[] negative_words = {"bad", "worst", "not", "unlikely", "worse", "beware", "horrible"};

            for (String word : negative_words) {

                if (line.contains(word))
                    context.write(new Text("The Negative # of feedback for Audi is :"), new IntWritable(1));

            }
        }
        String volvo = "volvo";
        if(line.contains(volvo)) {
            String[] positive_words = {"good", "great", "amazing", "awesome", "wonderful", "best"};

            for (String word : positive_words) {

                if (line.contains(word))
                    context.write(new Text("The Positive # of feedback for Volvo is :"), new IntWritable(1));

            }

            String[] negative_words = {"bad", "worst", "not", "unlikely", "worse", "beware", "horrible"};

            for (String word : negative_words) {

                if (line.contains(word))
                    context.write(new Text("The Negative # of feedback for Volvo is :"), new IntWritable(1));

            }
        }
        String tyo = "toyota";
        if(line.contains(tyo)) {
            String[] positive_words = {"good", "great", "amazing", "awesome", "wonderful", "best"};

            for (String word : positive_words) {

                if (line.contains(word))
                    context.write(new Text("The Positive # of feedback for Toyota is :"), new IntWritable(1));

            }

            String[] negative_words = {"bad", "worst", "not", "unlikely", "worse", "beware", "horrible"};

            for (String word : negative_words) {

                if (line.contains(word))
                    context.write(new Text("The Negative # of feedback for Toyota is :"), new IntWritable(1));

            }
        }
        String merc = "mercedes";
        if(line.contains(merc)) {
            String[] positive_words = {"good", "great", "amazing", "awesome", "wonderful", "best"};

            for (String word : positive_words) {

                if (line.contains(word))
                    context.write(new Text("The Positive # of feedback for Mercedes is :"), new IntWritable(1));

            }

            String[] negative_words = {"bad", "worst", "not", "unlikely", "worse", "beware", "horrible"};

            for (String word : negative_words) {

                if (line.contains(word))
                    context.write(new Text("The Negative # of feedback for Mercedes is :"), new IntWritable(1));

            }
        }
        String nissan = "nissan";
        if(line.contains(nissan)) {
            String[] positive_words = {"good", "great", "amazing", "awesome", "wonderful", "best"};

            for (String word : positive_words) {

                if (line.contains(word))
                    context.write(new Text("The Positive # of feedback for Nissan is :"), new IntWritable(1));

            }

            String[] negative_words = {"bad", "worst", "not", "unlikely", "worse", "beware", "horrible"};

            for (String word : negative_words) {

                if (line.contains(word))
                    context.write(new Text("The Negative # of feedback for Nissan is :"), new IntWritable(1));

            }
        }
        String tes = "tesla";
        if(line.contains(tes)) {
            String[] positive_words = {"good", "great", "amazing", "awesome", "wonderful", "best"};

            for (String word : positive_words) {

                if (line.contains(word))
                    context.write(new Text("The Positive # of feedback for Tesla is :"), new IntWritable(1));

            }

            String[] negative_words = {"bad", "worst", "not", "unlikely", "worse", "beware", "horrible"};

            for (String word : negative_words) {

                if (line.contains(word))
                    context.write(new Text("The Negative # of feedback for Tesla is :"), new IntWritable(1));

            }
        }

    }
}