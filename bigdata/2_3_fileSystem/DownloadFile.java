import java.net.URL;
import java.io.InputStream;
import java.io.FileOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;

public class DownloadFile {
	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	public static void main(String[] args) throws Exception {
		String sourceUrl = args[0];
		String destinationFile = args[1];	
	
		InputStream in = null;
		FileOutputStream fos = null;
			
		try {
			in = new URL(sourceUrl).openStream();
			fos = new FileOutputStream(destinationFile);
			IOUtils.copyBytes(in, fos, 4096, false);
		} finally {
			IOUtils.closeStream(in);
			fos.close();	
		}
	}
}
