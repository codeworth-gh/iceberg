package org.hilel14.iceberg;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.logging.Logger;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;

/**
 *
 * @author hilel14
 */
public class ArchiveCreator {

    private final Job job;
    ZipArchiveOutputStream zip;

    static final Logger LOGGER = Logger.getLogger(ArchiveCreator.class.getName());

    public ArchiveCreator(String jobFile) throws IOException {
        job = new Job(jobFile);
        System.out.println(job.getSource());
    }

    public void createArchive() throws IOException {
        zip = new ZipArchiveOutputStream(
                Files.createTempFile("icegerg.", ".zip").toFile());
        Files.walkFileTree(job.getSource(), new Zipper());
        zip.close();
    }

    class Zipper extends SimpleFileVisitor<Path> {

        @Override
        public FileVisitResult visitFile(Path path, BasicFileAttributes attrs)
                throws IOException {

            ZipArchiveEntry entry = new ZipArchiveEntry(path.toString());
            entry.setSize(Files.size(path));
            zip.putArchiveEntry(entry);
            zip.write(Files.readAllBytes(path));
            zip.closeArchiveEntry();

            return FileVisitResult.CONTINUE;
        }
    }

    /**
     * @return the job
     */
    public Job getJob() {
        return job;
    }
}
