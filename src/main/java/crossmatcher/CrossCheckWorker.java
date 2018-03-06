package crossmatcher;

import static crossmatcher.CrossCheckerApp.crossCheckSearchStringsInZipFiles;

import java.io.File;
import java.util.List;

public class CrossCheckWorker implements Runnable {

	private List<File> files;
	private String outputFolderPath;

	public CrossCheckWorker(List<File> files, String outputFolderPath) {
		this.files = files;
		this.outputFolderPath = outputFolderPath;
	}

	@Override
	public void run() {
		try {
			crossCheckSearchStringsInZipFiles(files, outputFolderPath);
		} catch (Exception e) {
			System.out.println(
					"Error happened with thread: " + Thread.currentThread().getName() + ", msg: " + e.getMessage());
		}
	}

}
