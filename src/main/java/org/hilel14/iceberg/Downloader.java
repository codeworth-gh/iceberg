package org.hilel14.iceberg;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.AmazonGlacierClientBuilder;
import com.amazonaws.services.glacier.model.GetJobOutputRequest;
import com.amazonaws.services.glacier.model.GetJobOutputResult;
import com.amazonaws.services.glacier.model.InitiateJobRequest;
import com.amazonaws.services.glacier.model.InitiateJobResult;
import com.amazonaws.services.glacier.model.JobParameters;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManagerBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.hilel14.iceberg.model.Inventory;

/**
 *
 * @author hilel14
 */
public class Downloader {

    static final Logger LOGGER = Logger.getLogger(Downloader.class.getName());
    AmazonGlacier client;
    ArchiveTransferManager transferManager;

    public Downloader(String glacierRegion) throws Exception {
        buildTransferManager(glacierRegion);
    }

    private void buildTransferManager(String glacierRegion) throws IOException {
        // build glacier client
        client = AmazonGlacierClientBuilder
                .standard()
                .withRegion(glacierRegion)
                .build();
        // build transfer manager
        transferManager = new ArchiveTransferManagerBuilder()
                .withGlacierClient(client)
                .build();
    }

    public void download(Path inventoryFile, Path targetFolder, String vaultName)
            throws Exception {
        Files.createDirectories(targetFolder);
        if (targetFolder.toFile().list().length > 0) {
            LOGGER.log(Level.WARNING, "Target folder {0} is not empty", targetFolder);
            //System.exit(1);
        }
        LOGGER.log(Level.INFO, "Parsing inventory file {0}", inventoryFile);
        ObjectMapper mapper = new ObjectMapper();
        Inventory inventory = mapper.readValue(inventoryFile.toFile(), Inventory.class);
        LOGGER.log(Level.INFO, "Inventory date: {0}", inventory.getInventoryDate());
        LOGGER.log(Level.INFO, "Vault ARN: {0}", inventory.getVaultArn());
        inventory.getArchiveList().forEach((map) -> {
            String archiveId = map.get("ArchiveId").toString();
            String date = map.get("CreationDate").toString().replaceAll(":", "-"); // 2017-12-08T11:59:02Z
            Path target = targetFolder.resolve(date + ".zip");
            if (!Files.exists(target)) {
                //LOGGER.log(Level.INFO, "Downloading archive {0} to file {1}", new Object[]{archiveId, target.getFileName()});
                //transferManager.download(vaultName, archiveId, target.toFile());
                String jobId = initiateJobRequest(vaultName, archiveId);
                LOGGER.log(Level.INFO, "Archive retrieval request with job id {0} initiated for archive {1}",
                        new Object[]{jobId, archiveId});
            }
        });
        LOGGER.log(Level.INFO, "The operation completed successfully");
    }

    private String initiateJobRequest(String vaultName, String archiveId) {

        JobParameters jobParameters = new JobParameters()
                .withType("archive-retrieval")
                .withArchiveId(archiveId);

        InitiateJobRequest request = new InitiateJobRequest()
                .withVaultName(vaultName)
                .withJobParameters(jobParameters);

        InitiateJobResult response = client.initiateJob(request);

        return response.getJobId();
    }

    public void downloadJobOutput(String vaultName, String jobId, Path targetFile) throws Exception {

        GetJobOutputRequest getJobOutputRequest = new GetJobOutputRequest()
                .withVaultName(vaultName)
                .withJobId(jobId);
        GetJobOutputResult getJobOutputResult = client.getJobOutput(getJobOutputRequest);

        byte[] buffer = new byte[1024 * 1024];
        try (InputStream input = new BufferedInputStream(getJobOutputResult.getBody());
                OutputStream output = new BufferedOutputStream(new FileOutputStream(targetFile.toFile()))) {
            int bytesRead;
            do {
                bytesRead = input.read(buffer);
                if (bytesRead <= 0) {
                    break;
                }
                output.write(buffer, 0, bytesRead);
            } while (bytesRead > 0);
        }
    }
}
