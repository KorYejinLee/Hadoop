import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

public class FindMaxTempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final int MISSING = 9999;

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String data = value.toString();
		String year = data.substring(15, 19);
		int airTemp;

		if (data.charAt(87)=='+') airTemp = Integer.parseInt(data.substring(88,92));
		else airTemp = Integer.parseInt(data.substring(87,92));

		String quality = data.substring(92, 93);

		if (airTemp != MISSING && quality.matches("[01459]"))
		{
			context.write(new Text(year), new IntWritable(airTemp));
		}
	}
}
