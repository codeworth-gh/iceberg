package org.hilel14.iceberg;

import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.AmazonGlacierClientBuilder;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManagerBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.hilel14.iceberg.model.Inventory;

/**
 *
 * @author hilel14
 */
public class Downloader {

    static final Logger LOGGER = Logger.getLogger(Downloader.class.getName());
    ArchiveTransferManager transferManager;

    public Downloader(String glacierRegion) throws Exception {
        buildTransferManager(glacierRegion);
    }

    private void buildTransferManager(String glacierRegion) throws IOException {
        // build glacier client
        AmazonGlacier glacier = AmazonGlacierClientBuilder
                .standard()
                .withRegion(glacierRegion)
                .build();
        // build transfer manager
        transferManager = new ArchiveTransferManagerBuilder()
                .withGlacierClient(glacier)
                .build();
    }

    public void download(Path inventoryFile, Path targetFolder, String vaultName)
            throws Exception {
        Files.createDirectories(targetFolder);
        if (targetFolder.toFile().list().length > 0) {
            LOGGER.log(Level.WARNING, "Target folder {0} is not empty", targetFolder);
            System.exit(1);
        }
        LOGGER.log(Level.INFO, "Parsing inventory file {0}", inventoryFile);
        ObjectMapper mapper = new ObjectMapper();
        Inventory inventory = mapper.readValue(inventoryFile.toFile(), Inventory.class);
        LOGGER.log(Level.INFO, "Inventory date: {0}", inventory.getInventoryDate());
        LOGGER.log(Level.INFO, "Vault ARN: {0}", inventory.getVaultArn());
        inventory.getArchiveList().forEach((map) -> {
            String archiveId = map.get("ArchiveId").toString();
            String date = map.get("CreationDate").toString().replaceAll(":", "-"); // 2017-12-08T11:59:02Z
            File targetFile = targetFolder.resolve(date + ".zip").toFile();
            LOGGER.log(Level.INFO, "Downloading archive {0} to file {1}",
                    new Object[]{archiveId, targetFile.getName()});
            transferManager.download(vaultName, archiveId, targetFile);
        });
        LOGGER.log(Level.INFO, "The operation completed successfully");
    }
}
