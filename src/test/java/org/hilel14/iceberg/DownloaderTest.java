package org.hilel14.iceberg;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.Test;

/**
 *
 * @author hilel14
 */
public class DownloaderTest {

    public DownloaderTest() {
    }

    @Test
    public void testDownloadJobOutput() throws Exception {
        Path source = Paths.get("/var/opt/data/iceberg/jobs.csv");
        Path target = Paths.get("/var/opt/data/iceberg/archives");
        List<String> lines = Files.readAllLines(source);
        Downloader instance = new Downloader("eu-west-1");
        String vaultName = "lenovo";
        for (String line : lines) {
            String[] parts = line.split(",");
            String jobId = parts[0];
            Path targetFile = target.resolve(parts[2] + ".zip");
            //System.out.println("jobId=" + jobId + " target=" + targetFile);
            instance.downloadJobOutput(vaultName, jobId, targetFile);
        }
    }

}
