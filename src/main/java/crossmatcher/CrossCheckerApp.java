package crossmatcher;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

public class CrossCheckerApp {

	private static final String MISSED_ZIPS_FILE = "missed_zips.txt";
	private static final int JOB_CHECK_RETRY_INTERVAL = 1000;
	private static final int ZIP_FILES_PER_THREAD = 2;
	private static Set<String> searchValues = new HashSet<>();
	private static List<String> missedZipMessages = new Vector<>();

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			throw new RuntimeException("Invalid number of arguments, must be 3!");
		}

		long start = new Date().getTime();
		System.out.println("Begin job: " + start);
		String searchFilePath = args[0];
		String inputZipFolderPath = args[1];
		String outputFolderPath = args[2];
		File searchFolder = new File(searchFilePath);
		File zipFolder = new File(inputZipFolderPath);
		File outputFolder = new File(outputFolderPath);
		asList(zipFolder, searchFolder, outputFolder).forEach(CrossCheckerApp::checkFolder);

		// read searchfile to memory, get in hash colelction is O(1)
		Files.lines(Paths.get(searchFolder.listFiles()[0].getAbsolutePath())).forEach(line -> searchValues.add(line));
		System.out.println("Driver textfile size: " + searchValues.size());

		ExecutorService executorService = Executors.newFixedThreadPool(ZIP_FILES_PER_THREAD * 20);
		List<File> filestoHandle = new ArrayList<>();
		List<Future<?>> runningThreads = new ArrayList<>();
		for (File file : zipFolder.listFiles()) {
			filestoHandle.add(file);
			if (filestoHandle.size() == ZIP_FILES_PER_THREAD) {
				fireCrossMatcher(outputFolderPath, executorService, filestoHandle.stream().collect(toList()),
						runningThreads);
				filestoHandle.clear();
			}
		}
		if (!filestoHandle.isEmpty()) {
			fireCrossMatcher(outputFolderPath, executorService, filestoHandle.stream().collect(toList()),
					runningThreads);
		}

		boolean isDone = false;
		int allRunninghreads = runningThreads.size();
		while (!isDone) {
			long completed = runningThreads.stream().filter(future -> future.isDone()).count();
			if (completed != allRunninghreads) {
				System.out.println("Waiting for threads to complete...");
				Thread.sleep(JOB_CHECK_RETRY_INTERVAL);
			} else {
				isDone = true;
				System.out.println("All thread completed!");
			}
		}

		System.out.println("Writing out missed zip messages...");
		try (FileWriter writer = new FileWriter(MISSED_ZIPS_FILE)) {
			writer.write("--- Missed zip files for cross checking at: " + new Date() + " ---\n");
			for (String str : missedZipMessages) {
				writer.write(str);
			}
		}

		executorService.shutdown();
		long end = new Date().getTime();
		System.out.println("End job: " + end);
		System.out.println("diff: " + (end - start) + " milliseconds");
	}

	static void crossCheckSearchStringsInZipFiles(List<File> fileInZipFolder, String outputFolderPath)
			throws Exception {
		for (File file : fileInZipFolder) {
			String name = file.getName();
			System.out.println("Started searching in: " + name);

			try (ZipFile zf = new ZipFile(file)) {
				ZipEntry singleFileInZip = (ZipEntry) zf.entries().nextElement();

				try (BufferedReader br = new BufferedReader(
						new InputStreamReader(zf.getInputStream(singleFileInZip)))) {
					boolean isContainsSearchString = false;
					for (String lineInZipFile = br.readLine(); lineInZipFile != null; lineInZipFile = br.readLine()) {
						if (searchValues.contains(lineInZipFile)) {
							System.out.println("Zip file \"" + name + "\" contains searchstring: " + lineInZipFile);
							isContainsSearchString = true;
							unzip(file.getAbsolutePath(), outputFolderPath);
							break;
						}
					}
					if (!isContainsSearchString) {
						missedZipMessages
								.add("Zip file \"" + name + "\" does not contain any input from searchstrings!\n");
					}
				}
			}
		}
	}

	private static void checkFolder(File f) {
		if (!f.exists() || !f.isDirectory()) {
			throw new RuntimeException("File " + f.getName() + " doest not exist or not a directory!");
		}
	}

	private static boolean fireCrossMatcher(String outputFolderPath, ExecutorService executorService,
			List<File> filestoHandle, List<Future<?>> runningThreads) {
		return runningThreads.add(executorService.submit(new CrossCheckWorker(filestoHandle, outputFolderPath)));
	}

	/**
	 * Generate random postfix for file just to be sure...
	 * 
	 * @param zipFile
	 * @param outputFolder
	 */
	private static void unzip(String zipFile, String outputFolder) {
		try (ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFile));) {
			StringBuilder b = new StringBuilder();
			b.append(outputFolder).append(File.separator).append(zis.getNextEntry().getName().split("\\.")[0])
					.append(new Random().nextInt(100000)).append(".txt");
			try (FileOutputStream fos = new FileOutputStream(new File(b.toString()))) {
				byte[] buffer = new byte[1024];
				int len;
				while ((len = zis.read(buffer)) > 0) {
					fos.write(buffer, 0, len);
				}
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		System.out.println("Done unzipping file: " + zipFile);
	}
}
