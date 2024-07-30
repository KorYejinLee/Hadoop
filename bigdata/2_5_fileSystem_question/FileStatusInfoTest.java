import java.io.*;
import org.junit.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class FileStatusInfoTest {
	private MiniDFSCluster dfs;
	private FileSystem filesystem;
		

	@Before
	public void setUp() throws IOException {
		Configuration conf = new Configuration();
		if (System.getProperty("test.build.data") == null) {
			System.setProperty("test.build.data", "/tmp");
		}
		dfs = new MiniDFSCluster.Builder(conf).build();
		filesystem = dfs.getFileSystem();
		OutputStream out = filesystem.create(new Path("/dir/file"));
		out.write("content".getBytes("UTF-8"));
	}
	
	@After
	public void tearDown() throws IOException {
		if (filesystem != null) { filesystem.close(); }
		if (dfs != null) { dfs.shutdown(); }
	}

	@Test(expected = FileNotFoundException.class)
	public void throwsFileNotFoundForNonExistentFile() throws IOException {
		filesystem.getFileStatus(new Path("no-such-file"));
	}

	@Test
	public void fileStatusForFile() throws IOException {
		Path file = new Path("/dir/file");
		FileStatus stat = filesystem.getFileStatus(file);

		assertThat(stat.getPath().toUri().getPath(), is("/dir/file"));
		assertThat(stat.isDirectory(), is(false));
		assertThat(stat.getLen(), is(7L));

		assertThat(stat.getModificationTime(), is(lessThanOrEqualTo(System.currentTimeMillis())));
		assertThat(stat.getReplication(), is((short) 1));
		assertThat(stat.getBlockSize(), is(128 * 1024 * 1024L));
		
		assertThat(stat.getOwner(), is(System.getProperty("user.name")));
		assertThat(stat.getGroup(), is("supergroup"));
		assertThat(stat.getPermission().toString(), is("rw-r--r--"));
	}

	@Test
	public void fileStatusForDirectory() throws IOException {
		Path path = new Path("/dir");
		FileStatus stat = filesystem.getFileStatus(path);
		
		assertThat(stat.getPath().toUri().getPath(), is("/dir"));
		assertThat(stat.isDirectory(), is(true));
		assertThat(stat.getLen(), is(0L));
		assertThat(stat.getModificationTime(),
		is(lessThanOrEqualTo(System.currentTimeMillis())));
		assertThat(stat.getReplication(), is((short) 0));
		assertThat(stat.getBlockSize(), is(0L));
		assertThat(stat.getOwner(), is(System.getProperty("user.name")));
		assertThat(stat.getGroup(), is("supergroup"));
		assertThat(stat.getPermission().toString(), is("rwxr-xr-x"));
	}
}
