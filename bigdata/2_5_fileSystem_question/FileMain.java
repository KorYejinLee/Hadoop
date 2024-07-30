import java.io.*;
import org.junit.*;

public class FileMain {
	public static void main(String[] args) throws Exception {
		FileStatusInfoTest test = new FileStatusInfoTest();

		test.setUp();
		test.fileStatusForFile();
		test.fileStatusForDirectory();
		test.tearDown();
	}
}
