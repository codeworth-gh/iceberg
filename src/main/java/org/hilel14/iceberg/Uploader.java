package org.hilel14.iceberg;

import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.AmazonGlacierClientBuilder;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManagerBuilder;
import java.io.IOException;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author hilel14
 */
public class Uploader {

    static final Logger LOGGER = Logger.getLogger(Uploader.class.getName());
    Job job;
    ArchiveTransferManager transferManager;

    public Uploader(Job job) throws IOException {
        this.job = job;
        buildTransferManager();
        LOGGER.log(Level.INFO,
                "Building glacier transfer manager "
                + "for {0} region, using default credentials",
                job.getGlacierRegion());
    }

    private void buildTransferManager() throws IOException {
        // build glacier client
        AmazonGlacier glacier = AmazonGlacierClientBuilder
                .standard()
                .withRegion(job.getGlacierRegion())
                .build();
        // build transfer manager
        transferManager = new ArchiveTransferManagerBuilder()
                .withGlacierClient(glacier)
                .build();
    }

    public void upload(Path archive) throws Exception {
        LOGGER.log(Level.SEVERE, "Uploading {0}", archive);
        transferManager.upload(job.getGlacierVault(), job.getId(), archive.toFile());
    }
}
