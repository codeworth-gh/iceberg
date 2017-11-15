package org.hilel14.glacier.backup;

import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.AmazonGlacierClientBuilder;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManagerBuilder;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import org.apache.commons.codec.binary.Base64;

/**
 *
 * @author hilel
 */
public class Uploader {

    static final Logger LOGGER = Logger.getLogger(Uploader.class.getName());

    // command line arguments
    Path digestsFile;
    Path sourceFolder;
    String vaultName;
    boolean dryRun;
    // configuration properties
    Pattern includePattern;
    // other properties
    List<String> digestList;
    ArchiveTransferManager transferManager;
    int matchCount = 0;
    int uploadCount = 0;

    public Uploader(Path assetsFolder, Path digestFile, Boolean dryRun)
            throws Exception {
        this.sourceFolder = assetsFolder;
        this.digestsFile = digestFile;
        this.dryRun = dryRun;
        digestList = Files.readAllLines(digestsFile);
        LOGGER.log(Level.INFO, "{0} digests found in upload history file", digestList.size());
        // load application properties
        Properties properties = new Properties();
        properties.load(Uploader.class.getResourceAsStream("/application.properties"));
        this.vaultName = properties.getProperty("glacier.vault.name");
        loadIncludePattern(properties);
        buildTransferManager(properties);
    }

    public void mainTask() throws IOException {
        LOGGER.log(Level.INFO, "Processing files in folder {0}", sourceFolder);
        LOGGER.log(Level.INFO, "Uploading to vault {0}", vaultName);
        LOGGER.log(Level.INFO, "Dry-run flag set to {0}", dryRun);
        Files.walkFileTree(sourceFolder, new Uploader.UploadVisitor());
        LOGGER.log(Level.INFO, "{0} matching files found, {1} uploaded", new Object[]{matchCount, uploadCount});
    }

    private ArchiveTransferManager buildTransferManager(Properties properties) throws IOException {
        String region = properties.getProperty("glacier.region.name");
        LOGGER.log(Level.INFO, "Building glacier transfer manager using default credentials and {0} region", region);
        // build glacier client
        AmazonGlacier glacier = AmazonGlacierClientBuilder
                .standard()
                .withRegion(region)
                .build();
        // build transfer manager
        transferManager = new ArchiveTransferManagerBuilder()
                .withGlacierClient(glacier)
                .build();
        // return the transfer manager
        return transferManager;
    }

    private void loadIncludePattern(Properties properties) throws IOException {
        String extentions = properties.getProperty("include.filename.extentions");
        StringBuilder b = new StringBuilder();
        b.append(".+"); // any character, one or more times
        b.append("(").append(extentions).append(")");
        //includePattern = Pattern.compile(".+?\\." + extentions);
        includePattern = Pattern.compile(b.toString());
        LOGGER.log(Level.INFO, "Including only files with extentions {0}", extentions);
    }

    class UploadVisitor extends SimpleFileVisitor<Path> {

        @Override
        public FileVisitResult visitFile(Path path, BasicFileAttributes attrs)
                throws IOException {
            if (includePattern.matcher(path.getFileName().toString().toLowerCase()).matches()) {
                matchCount++;
                try {
                    String digest = new String(MessageDigest.getInstance("MD5").digest(Files.readAllBytes(path)));
                    if (!digestList.contains(digest)) {
                        String description = Base64.encodeBase64String(path.toString().getBytes());
                        String archiveId = dryRun
                                ? path.getFileName().toString()
                                : transferManager.upload(vaultName, description, path.toFile()).getArchiveId();
                        LOGGER.log(Level.CONFIG, "File {0} with digest {1} uploaded as archive {2}",
                                new Object[]{path.getFileName(), digest, archiveId});
                        Files.write(digestsFile,
                                digest.concat(System.lineSeparator()).getBytes(),
                                StandardOpenOption.CREATE,
                                StandardOpenOption.APPEND);
                        uploadCount++;
                    }
                } catch (Exception ex) {
                    throw new IOException("Error when uploading file: " + path, ex);
                }
            }
            return FileVisitResult.CONTINUE;
        }

    }
}
