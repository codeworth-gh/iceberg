package org.hilel14.iceberg;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.commons.compress.utils.IOUtils;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.junit.BeforeClass;

/**
 *
 * @author hilel14
 */
public class ZipToolTest {

    static Path parentFolder;
    static Path sourceFolder;
    static Path workFolder;

    public ZipToolTest() {
    }

    @BeforeClass
    public static void setUpClass() throws IOException {
        parentFolder = Files.createTempDirectory("iceberg");
    }

    private void initJob(String testName) throws IOException {
        sourceFolder = parentFolder.resolve(testName).resolve("in");
        Files.createDirectories(sourceFolder);
        workFolder = parentFolder.resolve(testName).resolve("work");
        Files.createDirectories(workFolder);
        // deploy sample files
        String[] sampleFiles = new String[]{"doc1.txt", "doc2.txt", "doc3.html", "doc4.xml", ".doc5.txt", "doc-6.jpg"};
        for (String fileName : sampleFiles) {
            InputStream in = ZipToolTest.class.getResourceAsStream("/data/in/" + fileName);
            OutputStream out = new FileOutputStream(sourceFolder.resolve(fileName).toFile());
            IOUtils.copy(in, out);
        }
    }

    /**
     * Full backup with empty exclude filter
     *
     * @throws java.lang.Exception
     */
    @Test
    public void fullBackup() throws Exception {
        System.out.println("simple full backup");
        initJob("fullBackup");
        String regex = "";
        Pattern pattern = Pattern.compile(regex);
        ZipTool instance = new ZipTool(workFolder, "fullBackup", sourceFolder, pattern);
        instance.createArchive();
        assertEquals(5, instance.getFileCount());
    }

    /**
     * Full backup, excluding files that start with dot or end with xml or html
     *
     * @throws java.lang.Exception
     */
    @org.junit.Test
    public void excludeByPattern() throws Exception {
        System.out.println("full backup with filter");
        initJob("excludeByPattern");
        String regex = "\\..+|.+\\.(xml|html)";
        Pattern pattern = Pattern.compile(regex);
        ZipTool instance = new ZipTool(workFolder, "excludeByPattern", sourceFolder, pattern);
        instance.createArchive();
        assertEquals(2, instance.getFileCount());
    }

    /**
     * Incremental backup when no files were added or modified since last backup
     *
     * @throws java.lang.Exception
     */
    @org.junit.Test
    public void filesNotModified() throws Exception {
        System.out.println("incremental backup without new or modified files");
        initJob("filesNotModified");
        // start with full backup
        String regex = "";
        Pattern pattern = Pattern.compile(regex);
        ZipTool instance = new ZipTool(workFolder, "filesNotModified", sourceFolder, pattern);
        instance.createArchive();
        Path archive = instance.createArchive();
        assertEquals(5, instance.getFileCount());
        // change nothing and run incremental backup
        Files.move(archive, archive.getParent().resolve("1." + archive.getFileName()));
        instance = new ZipTool(workFolder, "filesNotModified", sourceFolder, pattern);
        instance.createArchive();
        assertEquals(0, instance.getFileCount());
        // copy, move and delete files, then run incremental backup again
        Files.move(archive, archive.getParent().resolve("2." + archive.getFileName()));
        Files.copy(sourceFolder.resolve("doc1.txt"), sourceFolder.resolve("doc11.txt"));
        Files.move(sourceFolder.resolve("doc2.txt"), sourceFolder.resolve("doc22.txt"));
        Files.delete(sourceFolder.resolve("doc3.html"));
        instance = new ZipTool(workFolder, "filesNotModified", sourceFolder, pattern);
        instance.createArchive();
        assertEquals(0, instance.getFileCount());
    }

    /**
     * Incremental backup with modified files
     *
     * @throws java.lang.Exception
     */
    @org.junit.Test
    public void modifiedFiles() throws Exception {
        System.out.println("incremental backup with modified files");
        initJob("modifiedFiles");
        // start with full backup
        String regex = "";
        Pattern pattern = Pattern.compile(regex);
        ZipTool instance = new ZipTool(workFolder, "modifiedFiles", sourceFolder, pattern);
        Path archive = instance.createArchive();
        assertEquals(5, instance.getFileCount());
        // modify a file
        Files.move(archive, archive.getParent().resolve("1." + archive.getFileName()));
        List<String> lines = new ArrayList<>();
        lines.add("This file was modified at " + new Date().toString());
        Files.write(sourceFolder.resolve("doc1.txt"), lines, StandardOpenOption.APPEND);
        instance = new ZipTool(workFolder, "modifiedFiles", sourceFolder, pattern);
        instance.createArchive();
        assertEquals(1, instance.getFileCount());
    }

}
