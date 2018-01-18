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

    public void uploadArchive(String region, String vault, String description, Path archive)
            throws Exception {
        LOGGER.log(Level.INFO, "Uploading {0} as {1} to {2} at {3} "
                + "using default credentials",
                new Object[]{archive, description, vault, region});
        // build glacier client
        AmazonGlacier amazonGlacier = AmazonGlacierClientBuilder
                .standard()
                .withRegion(region)
                .build();
        // build transfer manager
        ArchiveTransferManager transferManager = new ArchiveTransferManagerBuilder()
                .withGlacierClient(amazonGlacier)
                .build();
        // upload
        transferManager.upload(vault, description, archive.toFile());
    }

    // https://docs.aws.amazon.com/amazonglacier/latest/dev/downloading-an-archive-using-java.html
    public void initiateRetrievalRequests(Path inventoryFile) throws Exception {
        int count = 0;
        LOGGER.log(Level.INFO, "Parsing inventory file {0}", inventoryFile);
        Inventory inventory
                = new ObjectMapper().readValue(inventoryFile.toFile(), Inventory.class);
        Map<String, String> arn = inventory.extractVaultArn();
        String region = arn.get("region");
        String vault = arn.get("vault");
        LOGGER.log(Level.INFO, "Inventory date: {0} region {1} vault {2}",
                new Object[]{inventory.getInventoryDate(), region, vault});
        // build glacier client
        AmazonGlacier glacier = AmazonGlacierClientBuilder
                .standard()
                .withRegion(region)
                .build();
        // initiate requests and save results in a file
        Path target = inventoryFile.getParent().resolve(region).resolve(vault).resolve("retrieval-requests.csv");
        Files.createDirectories(target.getParent());
        try (BufferedWriter writer = Files.newBufferedWriter(target, StandardOpenOption.CREATE_NEW)) {
            for (Map map : inventory.getArchiveList()) {
                String archiveId = map.get("ArchiveId").toString();
                String date = map.get("CreationDate").toString().replaceAll(":", "-");
                String jobId = initiateRetrievalRequest(glacier, vault, archiveId);
                writer.write(jobId + "," + date.concat(".zip"));
                writer.newLine();
                count++;
            }
            LOGGER.log(Level.INFO, "{0} retrieval requests initiated and saved in file {1}",
                    new Object[]{count, target});
        }
    }

    private String initiateRetrievalRequest(AmazonGlacier glacier, String vault, String archiveId) {
        // build job parameters
        JobParameters jobParameters = new JobParameters()
                .withType("archive-retrieval")
                .withArchiveId(archiveId);
        InitiateJobRequest request = new InitiateJobRequest()
                .withVaultName(vault)
                .withJobParameters(jobParameters);
        // send request and get job id
        String result = glacier
                .initiateJob(request)
                .getJobId();
        // return
        return result;
    }

    public void retrieveArchives(Path inputFile, Path targetFolder, String region, String vault)
            throws Exception {
        LOGGER.log(Level.INFO, "downloading archives in file {0}", inputFile);
        // build glacier client
        AmazonGlacier glacier = AmazonGlacierClientBuilder
                .standard()
                .withRegion(region)
                .build();
        // retrive all archives in requests file
        Files.createDirectories(targetFolder);
        List<String> retrievalJobs = Files.readAllLines(inputFile);
        for (String retrievalJob : retrievalJobs) {
            String[] parts = retrievalJob.split(",");
            String jobId = parts[0];
            String targetFile = parts[1];
            retrieveArchive(glacier, vault, jobId, targetFolder.resolve(targetFile));
        }
        LOGGER.log(Level.INFO, "{0} archives downloaded to {1}",
                new Object[]{retrievalJobs.size(), targetFolder});
    }

    private void retrieveArchive(AmazonGlacier glacier, String vault,
            String jobId, Path targetFile)
            throws Exception {
        GetJobOutputRequest getJobOutputRequest
                = new GetJobOutputRequest()
                        .withVaultName(vault)
                        .withJobId(jobId);
        GetJobOutputResult getJobOutputResult
                = glacier.getJobOutput(getJobOutputRequest);
        Files.copy(getJobOutputResult.getBody(), targetFile);
    }
}
