import java.net.URI;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;

public class ShowFileStatus {
	public static void main(String[] args) throws Exception {
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem filesystem = FileSystem.get(URI.create(uri), conf);
		Path[] arrPath = new Path[args.length];
		
		for (int i = 0; i < arrPath.length; ++i) {
			arrPath[i] = new Path(args[i]);
		}
	
		FileStatus[] fileStatus = filesystem.listStatus(arrPath);
		Path[] listedPaths =  FileUtil.stat2Paths(fileStatus);

		for (Path dir : listedPaths) {
			System.out.println(dir);
		}
	}
}
