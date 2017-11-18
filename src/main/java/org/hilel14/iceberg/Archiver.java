package org.hilel14.iceberg;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;

/**
 *
 * @author hilel14
 */
public class Archiver {

    Job job;
    Path workFolder;
    Path snapshotFile;
    Snapshot snapshot = new Snapshot();
    private ZipArchiveOutputStream zip;
    private long fileCount = 0;

    static final Logger LOGGER = Logger.getLogger(Archiver.class.getName());

    public Archiver(Job job, Path workFolder)
            throws IOException {
        LOGGER.log(Level.INFO, "Preparing to run job {0}", job.getId());
        this.job = job;
        this.workFolder = workFolder;
        // init snapshot
        snapshotFile = workFolder.resolve(job.getId() + ".snapshot.json");
        snapshot.load(snapshotFile);
        snapshot.getHashToPaths().clear();
    }

    public Path createArchive() throws IOException {
        LOGGER.log(Level.INFO, "collecting files from {0} excluding pattern {1}",
                new Object[]{job.getSourceFolder(), job.getExcludeFilter()});
        //Path target = Files.createTempFile("iceberg.", ".zip");
        Path target = workFolder.resolve(job.getId() + ".zip");
        Files.deleteIfExists(target);
        LOGGER.log(Level.INFO, "Adding files to {0}", target);
        zip = new ZipArchiveOutputStream(target.toFile());
        Files.walkFileTree(job.getSourceFolder(), new Zipper());
        LOGGER.log(Level.INFO, "{0} files added, archive size is {1} bytes",
                new Object[]{fileCount, Files.size(target)});
        snapshot.save(snapshotFile);
        add(snapshotFile);
        zip.close();
        return target;
    }

    class Zipper extends SimpleFileVisitor<Path> {

        @Override
        public FileVisitResult visitFile(Path path, BasicFileAttributes attrs)
                throws IOException {
            // exclude
            if (job.getExcludeFilter().matcher(path.getFileName().toString()).matches()) {
                return FileVisitResult.CONTINUE;
            }
            // calculate file hash
            String hash;
            try (InputStream in = new FileInputStream(path.toFile())) {
                hash = DigestUtils.md5Hex(in);
            }
            // add new files to archive
            if (!snapshot.getFileHashes().contains(hash)) {
                add(path);
                fileCount++;
            }
            // update snapshot and continue
            if (snapshot.getHashToPaths().containsKey(hash)) {
                snapshot.getHashToPaths().get(hash).add(path);
            } else {
                Set<Path> paths = new HashSet<>();
                paths.add(path);
                snapshot.getHashToPaths().put(hash, paths);
            }

            return FileVisitResult.CONTINUE;
        }
    }

    private void add(Path path) throws IOException {
        ZipArchiveEntry entry = new ZipArchiveEntry(path.toString());
        entry.setSize(Files.size(path));
        zip.putArchiveEntry(entry);
        zip.write(Files.readAllBytes(path));
        zip.closeArchiveEntry();
    }

    /**
     * @return the fileCount
     */
    public long getFileCount() {
        return fileCount;
    }

}
