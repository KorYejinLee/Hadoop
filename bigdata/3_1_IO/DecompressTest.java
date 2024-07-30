import java.net.URI;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

public class DecompressTest {
	public static void main(String[] args) throws Exception {
		String uri = args[0];
		Configuration cf = new Configuration();
		FileSystem filesystem = FileSystem.get(URI.create(uri),cf);
		Path inputPath = new Path(uri);
		CompressionCodecFactory factory = new CompressionCodecFactory(cf);
		CompressionCodec codec = factory.getCodec(inputPath);
		if (codec == null) {
			System.err.println("No codec found for " + uri);
			System.exit(1);
		}
		String outputUri = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());
		InputStream is = null;
		OutputStream os = null;
		try {
			is = codec.createInputStream(filesystem.open(inputPath));
			os = filesystem.create(new Path(outputUri));
			IOUtils.copyBytes(is, os, cf);
		} finally {
			IOUtils.closeStream(is);	
			IOUtils.closeStream(os);
		}
	}
}
