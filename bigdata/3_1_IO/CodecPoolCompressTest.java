import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

public class CodecPoolCompressTest {
	public static void main(String[] args) throws Exception {
		String codecName = args[0];
		Class<?> codecClass = Class.forName(codecName);
		Configuration cf = new Configuration();

		CompressionCodec cd = (CompressionCodec) ReflectionUtils.newInstance(codecClass, cf);
		Compressor compressor = null;
		
		try {
			compressor = CodecPool.getCompressor(cd);
			CompressionOutputStream os = cd.createOutputStream(System.out, compressor);
			IOUtils.copyBytes(System.in, os, 4096, false);
			os.finish();
		} finally {
			CodecPool.returnCompressor(compressor);
		}
	}
}
