import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LikeMapperOLD
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


	if (cars[2].contains(audi))
        	context.write(new Text("Total # of likes for Audi is :"), new Text(cars[0]));
	if (cars[2].contains(tes))
        	context.write(new Text("Total # of likes for Tesla is :"), new Text(cars[0]));
	if (cars[2].contains(merc))
        	context.write(new Text("Total # of likes for Mercedes is :"), new Text(cars[0]));
	if (cars[2].contains(nissan))
        	context.write(new Text("Total # of likes for Nissan is :"), new Text(cars[0]));
	if (cars[2].contains(vol))
        	context.write(new Text("Total # of likes for Volvo is :"), new Text(cars[0]));
	if (cars[2].contains(toy))
        	context.write(new Text("Total # of likes for Toyota is :"), new Text(cars[0]));

	
        
    }
}