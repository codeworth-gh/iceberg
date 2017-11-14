package org.hilel14.iceberg;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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

    private final Job job;
    ZipArchiveOutputStream zip;
    MessageDigest digest;

    static final Logger LOGGER = Logger.getLogger(Archiver.class.getName());

    public Archiver(String jobFile)
            throws IOException, NoSuchAlgorithmException {
        job = new Job(jobFile);
        digest = MessageDigest.getInstance("MD5");
        Snapshot snapshot = new Snapshot();
        snapshot.load(job.getSnapshotPath());
        snapshot.getHashToPaths().clear();
        job.setSnapshot(snapshot);
    }

    public Path createArchive() throws IOException {
        Path target = Files.createTempFile("icegerg.", ".zip");
        LOGGER.log(Level.INFO,
                "Adding files in {0} to {1}",
                new Object[]{job.getSource(), target});
        zip = new ZipArchiveOutputStream(target.toFile());
        Files.walkFileTree(job.getSource(), new Zipper());
        zip.close();
        job.getSnapshot().save(job.getSnapshotPath());
        return target;
    }

    class Zipper extends SimpleFileVisitor<Path> {

        @Override
        public FileVisitResult visitFile(Path path, BasicFileAttributes attrs)
                throws IOException {
            // calculate file hash
            String hash;
            try (InputStream in = new FileInputStream(path.toFile())) {
                hash = DigestUtils.md5Hex(in);
            }
            // add new files to archive
            if (!job.getSnapshot().getFileHashes().contains(hash)) {
                ZipArchiveEntry entry = new ZipArchiveEntry(path.toString());
                entry.setSize(Files.size(path));
                zip.putArchiveEntry(entry);
                zip.write(Files.readAllBytes(path));
                zip.closeArchiveEntry();
            }

            // update snapshot and continue            
            if (job.getSnapshot().getHashToPaths().containsKey(hash)) {
                job.getSnapshot().getHashToPaths().get(hash).add(path);
            } else {
                Set<Path> paths = new HashSet<>();
                paths.add(path);
                job.getSnapshot().getHashToPaths().put(hash, paths);
            }

            return FileVisitResult.CONTINUE;
        }
    }

}
