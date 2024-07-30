import java.net.URI;
import java.io.InputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;

public class DisplayFileFS
{
	public static void main(String[] args) throws Exception
	{
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem filesystem = FileSystem.get(URI.create(uri), conf);
		InputStream in = null;
		try
		{
			in = filesystem.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);
		}
		finally
		{
			IOUtils.closeStream(in);
		}
	}
}


