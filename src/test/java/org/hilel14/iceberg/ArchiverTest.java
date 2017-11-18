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
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 *
 * @author hilel14
 */
public class ArchiverTest {

    String[] sampleFiles = new String[]{"doc1.txt", "doc2.txt", "doc3.html", "doc4.xml", ".doc5.txt"};
    static Path parentFolder;

    public ArchiverTest() throws IOException {
    }

    @BeforeClass
    public static void setUpClass() throws IOException {
        parentFolder = Files.createTempDirectory("iceberg");
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {

    }

    @After
    public void tearDown() {
    }

    private Job initJob(String id) throws IOException {
        Path sourceFolder = parentFolder.resolve(id).resolve("in");
        Files.createDirectories(sourceFolder);
        deploySampleFiles(sourceFolder);
        Job job = new Job();
        job.setId(id);
        job.setSourceFolder(sourceFolder);
        return job;
    }

    private void deploySampleFiles(Path sourceFolder) throws IOException {
        for (String fileName : sampleFiles) {
            InputStream in = ArchiverTest.class.getResourceAsStream("/data/in/" + fileName);
            OutputStream out = new FileOutputStream(sourceFolder.resolve(fileName).toFile());
            IOUtils.copy(in, out);
        }
    }

    /**
     * Full backup with empty exclude filter
     *
     * @throws java.lang.Exception
     */
    @org.junit.Test
    public void fullBackup() throws Exception {
        System.out.println("simple full backup");
        Job job = initJob("fullBackup");
        String regex = "";
        Pattern pattern = Pattern.compile(regex);
        job.setExcludeFilter(pattern);
        Archiver instance = new Archiver(job, job.getSourceFolder().getParent());
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
        Job job = initJob("excludeByPattern");
        String regex = "\\..+|.+\\.(xml|html)";
        Pattern pattern = Pattern.compile(regex);
        job.setExcludeFilter(pattern);
        Archiver instance = new Archiver(job, job.getSourceFolder().getParent());
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
        Job job = initJob("filesNotModified");
        // start with full backup
        String regex = "";
        Pattern pattern = Pattern.compile(regex);
        job.setExcludeFilter(pattern);
        Archiver instance = new Archiver(job, job.getSourceFolder().getParent());
        Path archive = instance.createArchive();
        assertEquals(5, instance.getFileCount());
        // change nothing and run incremental backup
        Files.move(archive, archive.getParent().resolve("1." + archive.getFileName()));
        instance = new Archiver(job, job.getSourceFolder().getParent());
        instance.createArchive();
        assertEquals(0, instance.getFileCount());
        // copy, move and delete files, then run incremental backup again
        Files.move(archive, archive.getParent().resolve("2." + archive.getFileName()));
        Files.copy(job.getSourceFolder().resolve("doc1.txt"), job.getSourceFolder().resolve("doc11.txt"));
        Files.move(job.getSourceFolder().resolve("doc2.txt"), job.getSourceFolder().resolve("doc22.txt"));
        Files.delete(job.getSourceFolder().resolve("doc3.html"));
        instance = new Archiver(job, job.getSourceFolder().getParent());
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
        Job job = initJob("modifiedFiles");
        // start with full backup
        String regex = "";
        Pattern pattern = Pattern.compile(regex);
        job.setExcludeFilter(pattern);
        Archiver instance = new Archiver(job, job.getSourceFolder().getParent());
        Path archive = instance.createArchive();
        assertEquals(5, instance.getFileCount());
        // modify a file
        Files.move(archive, archive.getParent().resolve("1." + archive.getFileName()));
        List<String> lines = new ArrayList<>();
        lines.add("This file was modified at " + new Date().toString());
        Files.write(job.getSourceFolder().resolve("doc1.txt"), lines, StandardOpenOption.APPEND);
        instance = new Archiver(job, job.getSourceFolder().getParent());
        instance.createArchive();
        assertEquals(1, instance.getFileCount());
    }

}
