package org.hilel14.iceberg;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.utils.IOUtils;
import static org.junit.Assert.*;
import org.junit.Before;

/**
 *
 * @author hilel14
 */
public class ArchiverTest {

    String[] sampleFiles = new String[]{"doc1.txt", "doc2.txt", "doc3.html", "doc4.xml", ".doc5.txt"};
    Path workFolder;
    Path inFolder;
    Path snapshotFile;

    @Before
    public void beforeTest() throws Exception {
        // create work folder for this test
        workFolder = Files.createTempDirectory("iceberg");
        // set path to snapshot file
        snapshotFile = workFolder.resolve("iceberg.snapshot.json");
        // create input folder
        inFolder = workFolder.resolve("in");
        Files.createDirectory(inFolder);
        // copy sample files to input folder
        deploySampleFiles();
    }

    /**
     * Full backup with empty exclude filter
     *
     * @throws java.lang.Exception
     */
    @org.junit.Test
    public void fullBackup() throws Exception {
        System.out.println("simple full backup");
        String regex = "";
        Pattern pattern = Pattern.compile(regex);
        Archiver instance = new Archiver(snapshotFile, inFolder, pattern);
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
        String regex = "\\..+|.+\\.(xml|html)";
        Pattern pattern = Pattern.compile(regex);
        Archiver instance = new Archiver(snapshotFile, inFolder, pattern);
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
        // start with full backup
        String regex = "";
        Pattern pattern = Pattern.compile(regex);
        Archiver instance = new Archiver(snapshotFile, inFolder, pattern);
        instance.createArchive();
        assertEquals(5, instance.getFileCount());
        // change nothing and run incremental backup
        instance = new Archiver(snapshotFile, inFolder, pattern);
        instance.createArchive();
        assertEquals(0, instance.getFileCount());
        // copy, move and delete files, then run incremental backup again
        Files.copy(inFolder.resolve("doc1.txt"), inFolder.resolve("doc11.txt"));
        Files.move(inFolder.resolve("doc2.txt"), inFolder.resolve("doc22.txt"));
        Files.delete(inFolder.resolve("doc3.html"));
        instance = new Archiver(snapshotFile, inFolder, pattern);
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
        // start with full backup
        String regex = "";
        Pattern pattern = Pattern.compile(regex);
        Archiver instance = new Archiver(snapshotFile, inFolder, pattern);
        instance.createArchive();
        assertEquals(5, instance.getFileCount());
        // modify a file
        List<String> lines = new ArrayList<>();
        lines.add("This file was modified at " + new Date().toString());
        Files.write(inFolder.resolve("doc1.txt"), lines, StandardOpenOption.APPEND);
        instance = new Archiver(snapshotFile, inFolder, pattern);
        instance.createArchive();
        assertEquals(1, instance.getFileCount());
    }

    private void deploySampleFiles() throws IOException {
        for (String fileName : sampleFiles) {
            InputStream in = ArchiverTest.class.getResourceAsStream("/data/in/" + fileName);
            OutputStream out = new FileOutputStream(inFolder.resolve(fileName).toFile());
            IOUtils.copy(in, out);
        }
    }
}
