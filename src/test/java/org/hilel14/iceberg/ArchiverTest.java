package org.hilel14.iceberg;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
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
        // extract sample files to input folder
        extractSampleFiles();
    }

    /**
     * Full backup with empty exclude filter
     *
     * @throws java.lang.Exception
     */
    @org.junit.Test
    public void excludeNothing() throws Exception {
        System.out.println("excludeNothing");
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
        System.out.println("excludeByPattern");
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
    public void noNewsGoodNews() throws Exception {
        System.out.println("noNewsGoodNews");
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
    public void incremental() throws Exception {
        System.out.println("incremental");
        // start with full backup
        String regex = "";
        Pattern pattern = Pattern.compile(regex);
        Archiver instance = new Archiver(snapshotFile, inFolder, pattern);
        instance.createArchive();
        assertEquals(5, instance.getFileCount());
        // modify a file
        List<String> lines = new ArrayList<>();
        lines.add("This file was modified");
        lines.add(new Date().toString());
        Files.write(inFolder.resolve("doc1.txt"), lines, StandardOpenOption.APPEND);
        instance = new Archiver(snapshotFile, inFolder, pattern);
        instance.createArchive();
        assertEquals(1, instance.getFileCount());
    }

    private void extractSampleFiles() throws Exception {
        InputStream in = ArchiverTest.class.getResourceAsStream("/data/in.zip");
        ArchiveStreamFactory factory = new ArchiveStreamFactory();
        try (ArchiveInputStream archiveInputStream
                = factory.createArchiveInputStream(ArchiveStreamFactory.ZIP, in)) {
            ZipArchiveEntry entry = (ZipArchiveEntry) archiveInputStream.getNextEntry();
            while (entry != null) {
                File outputFile = inFolder.resolve(entry.getName()).toFile();
                try (OutputStream outStream = new FileOutputStream(outputFile)) {
                    IOUtils.copy(archiveInputStream, outStream);
                }
                entry = (ZipArchiveEntry) archiveInputStream.getNextEntry();
            }
        }
    }
}
