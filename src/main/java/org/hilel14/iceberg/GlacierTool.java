package org.hilel14.iceberg;

import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.AmazonGlacierClientBuilder;
import com.amazonaws.services.glacier.model.GetJobOutputRequest;
import com.amazonaws.services.glacier.model.GetJobOutputResult;
import com.amazonaws.services.glacier.model.InitiateJobRequest;
import com.amazonaws.services.glacier.model.JobParameters;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManagerBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author hilel14
 */
public class GlacierTool {

    static final Logger LOGGER = Logger.getLogger(GlacierTool.class.getName());
    final AmazonGlacier amazonGlacier;
    final ArchiveTransferManager transferManager;

    public GlacierTool(String region, String vault) {
        // build glacier client
        amazonGlacier = AmazonGlacierClientBuilder
                .standard()
                .withRegion(region)
                .build();
        // build transfer manager
        transferManager = new ArchiveTransferManagerBuilder()
                .withGlacierClient(amazonGlacier)
                .build();
        // info
        LOGGER.log(Level.INFO,
                "Building glacier client and transfer manager, "
                + "using default credentials and {0} as region", region);
    }

    public void uploadArchive(String vault, String description, Path archive)
            throws Exception {
        LOGGER.log(Level.INFO, "Uploading {0}", archive);
        transferManager.upload(vault, description, archive.toFile());
    }

    // https://docs.aws.amazon.com/amazonglacier/latest/dev/downloading-an-archive-using-java.html
    public void initiateRetrievalRequests(Path inventoryFile) throws Exception {
        int count = 0;
        LOGGER.log(Level.INFO, "Parsing inventory file {0}", inventoryFile);
        ObjectMapper mapper = new ObjectMapper();
        Inventory inventory = mapper.readValue(inventoryFile.toFile(), Inventory.class);
        LOGGER.log(Level.INFO, "Inventory date: {0}", inventory.getInventoryDate());
        LOGGER.log(Level.INFO, "Vault ARN: {0}", inventory.getVaultArn());
        String vaultName = inventory.getVaultName();
        Path target = inventoryFile.getParent().resolve(vaultName + ".retrieval-requests.csv");
        try (BufferedWriter writer = Files.newBufferedWriter(target, StandardOpenOption.CREATE_NEW)) {
            for (Map map : inventory.getArchiveList()) {
                String archiveId = map.get("ArchiveId").toString();
                String date = map.get("CreationDate").toString().replaceAll(":", "-");
                String jobId = initiateRetrievalRequest(vaultName, archiveId);
                writer.write(jobId + "," + date.concat(".zip"));
                count++;
            }
            LOGGER.log(Level.INFO, "{0} retrieval requests initiated", count);
        }
    }

    private String initiateRetrievalRequest(String vaultName, String archiveId) {
        JobParameters jobParameters = new JobParameters()
                .withType("archive-retrieval")
                .withArchiveId(archiveId);
        InitiateJobRequest request = new InitiateJobRequest()
                .withVaultName(vaultName)
                .withJobParameters(jobParameters);
        return amazonGlacier
                .initiateJob(request)
                .getJobId();
    }

    public void retrieveArchives(Path inputFile) throws Exception {
        String vaultName = inputFile.getFileName().toString().split("-")[0];
        Path targetFolder = inputFile.getParent().resolve("archives");
        Files.createDirectories(targetFolder);
        List<String> retrievalJobs = Files.readAllLines(inputFile);
        for (String retrievalJob : retrievalJobs) {
            String[] parts = retrievalJob.split(",");
            String jobId = parts[0];
            String targetFile = parts[1];
            retrieveArchive(vaultName, jobId, targetFolder.resolve(targetFile));
        }
        LOGGER.log(Level.INFO, "{0} archives retrieved", retrievalJobs.size());
    }

    private void retrieveArchive(String vaultName, String jobId, Path targetFile) throws Exception {
        GetJobOutputRequest getJobOutputRequest
                = new GetJobOutputRequest()
                        .withVaultName(vaultName)
                        .withJobId(jobId);
        GetJobOutputResult getJobOutputResult
                = amazonGlacier.getJobOutput(getJobOutputRequest);
        Files.copy(getJobOutputResult.getBody(), targetFile);
    }
}
