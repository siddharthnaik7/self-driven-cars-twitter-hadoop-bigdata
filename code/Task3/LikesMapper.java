import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LikesMapper
        extends Mapper<LongWritable, Text, Text, Text> {


    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().toLowerCase();
	String[] cars = line.split("\\t");
	String audi = "audi";
	String tes = "tesla";
	String merc = "mercedes";
	String nissan = "nissan";
	String vol = "volvo";
	String toy = "toyota";

	if (cars[2].contains(audi)) {
            String[] positive_words = {"good", "great", "amazing", "awesome", "wonderful", "best"};

            for (String word : positive_words) {

                if (cars[2].contains(word))
                    context.write(new Text("Total # of likes for Positive Audi Tweets :"), new Text(cars[0]));

            }

            String[] negative_words = {"bad", "worst", "not", "unlikely", "worse", "beware", "horrible"};

            for (String word : negative_words) {

                if (cars[2].contains(word))
                    context.write(new Text("Total # of likes for Negative Audi Tweets :"), new Text(cars[0]));


            }
        }

        if (cars[2].contains(vol)) {
            String[] positive_words = {"good", "great", "amazing", "awesome", "wonderful", "best"};

            for (String word : positive_words) {

                if (cars[2].contains(word))
                    context.write(new Text("Total # of likes for Positive Volvo Tweets :"), new Text(cars[0]));

            }

            String[] negative_words = {"bad", "worst", "not", "unlikely", "worse", "beware", "horrible"};

            for (String word : negative_words) {

                if (cars[2].contains(word))
                    context.write(new Text("Total # of likes for Negative Volvo Tweets :"), new Text(cars[0]));

            }
        }

        if (cars[2].contains(toy)) {
            String[] positive_words = {"good", "great", "amazing", "awesome", "wonderful", "best"};

            for (String word : positive_words) {

                if (cars[2].contains(word))
                    context.write(new Text("Total # of likes for Positive Toyota Tweets :"), new Text(cars[0]));

            }

            String[] negative_words = {"bad", "worst", "not", "unlikely", "worse", "beware", "horrible"};

            for (String word : negative_words) {

                if (cars[2].contains(word))
                    context.write(new Text("Total # of likes for Negative Toyota Tweets :"), new Text(cars[0]));

            }
        }

        if(cars[2].contains(merc)) {
            String[] positive_words = {"good", "great", "amazing", "awesome", "wonderful", "best"};

            for (String word : positive_words) {

                if (cars[2].contains(word))
                    context.write(new Text("Total # of likes for Positive Mercedes Tweets :"), new Text(cars[0]));

            }

            String[] negative_words = {"bad", "worst", "not", "unlikely", "worse", "beware", "horrible"};

            for (String word : negative_words) {

                if (cars[2].contains(word))
                    context.write(new Text("Total # of likes for Negative Mercedes Tweets :"), new Text(cars[0]));

            }
        }

        if(cars[2].contains(nissan)) {
            String[] positive_words = {"good", "great", "amazing", "awesome", "wonderful", "best"};

            for (String word : positive_words) {

                if (cars[2].contains(word))
                    context.write(new Text("Total # of likes for Positive Nissan Tweets :"), new Text(cars[0]));

            }

            String[] negative_words = {"bad", "worst", "not", "unlikely", "worse", "beware", "horrible"};

            for (String word : negative_words) {

                if (cars[2].contains(word))
                    context.write(new Text("Total # of likes for Negative Nissan Tweets :"), new Text(cars[0]));

            }
        }

        if(cars[2].contains(tes)) {
            String[] positive_words = {"good", "great", "amazing", "awesome", "wonderful", "best"};

            for (String word : positive_words) {

                if (cars[2].contains(word))
                    context.write(new Text("Total # of likes for Positive Tesla Tweets :"), new Text(cars[0]));

            }

            String[] negative_words = {"bad", "worst", "not", "unlikely", "worse", "beware", "horrible"};

            for (String word : negative_words) {

                if (cars[2].contains(word))
                    context.write(new Text("Total # of likes for Negative Tesla Tweets :"), new Text(cars[0]));

            }
        }

	
        
    }
}