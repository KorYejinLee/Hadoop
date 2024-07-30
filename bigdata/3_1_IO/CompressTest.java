import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec; // 압축코덱
import org.apache.hadoop.io.compress.CompressionOutputStream; // 출력
public class CompressTest
{
	public static void main(String[] args) throws Exception // 메인함수
	{
		String codecClassname = args[0];
		Class<?> classCodec = Class.forName(codecClassname);
		Configuration cf = new Configuration();
		CompressionCodec compress = (CompressionCodec)
		ReflectionUtils.newInstance(classCodec, cf);
		CompressionOutputStream out = compress.createOutputStream(System.out);
		IOUtils.copyBytes(System.in, out, 4096, false);
		out.finish();
	}
}
