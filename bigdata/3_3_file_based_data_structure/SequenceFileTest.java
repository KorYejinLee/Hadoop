import java.net.URI;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.conf.Configuration;

public class SequenceFileTest {
	private static final String[] STR = {
		"One Little, Two Little, Three Little Indians ",
		"Four Little, Five Little, Six Little Indians ",
		"Seven Little, Eight Little, Nine Little Indians ",
		"Ten Little Indian Boys. ",
	};
	public static void main(String[] args) throws IOException {
		String uri = args[0];
		Configuration cf = new Configuration();
		FileSystem filesystem = FileSystem.get(URI.create(uri), cf);
		Path path = new Path(uri);
		IntWritable key = new IntWritable();
		Text value = new Text();
		SequenceFile.Writer writer = null;
		
		try {
			writer = SequenceFile.createWriter(filesystem, cf, path, key.getClass(), value.getClass());
			for (int i = 0; i < 100; ++i) {
				key.set(100 - i);
				value.set(STR[i % STR.length]);
				System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
				writer.append(key, value);
			}
		} finally {
			IOUtils.closeStream(writer);
		}
	}
}	
		
