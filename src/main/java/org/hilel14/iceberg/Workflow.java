package org.hilel14.iceberg;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * This class hold the logic of all backup and restore operations.
 *
 * @author hilel14
 */
public class Workflow {

    static final Logger LOGGER = Logger.getLogger(Workflow.class.getName());
    private String jobName;
    // application properties
    private Path workFolder;
    // job properties
    private Path sourceFolder;
    private Pattern excludeFilter;
    private String glacierRegion;
    private String glacierVault;

    public Workflow() {

    }

    public Workflow(String jobName) throws Exception {
        this.jobName = jobName;
        // load global application properties
        Properties p = new Properties();
        p.load(Workflow.class.getResourceAsStream("/iceberg.properties"));
        workFolder = Paths.get(p.getProperty("work.folder"));
        // load current job properties
        p.load(Workflow.class.getResourceAsStream("/jobs/" + jobName + ".properties"));
        sourceFolder = Paths.get(p.getProperty("source.folder"));
        excludeFilter = Pattern.compile(p.getProperty("exclude.pattern"));
        glacierRegion = p.getProperty("glacier.region");
        glacierVault = p.getProperty("glacier.vault");
    }

    /**
     * Create a Zip file containing all files that changed since last backup.
     * Optionally use filter to exclude some files. Optionally upload to
     * Glacier.
     *
     * @param upload
     * @throws java.lang.Exception
     */
    public void createArchive(boolean upload) throws Exception {
        ZipTool zipTool = new ZipTool(workFolder, jobName, sourceFolder, excludeFilter);
        Path zipFile = zipTool.createArchive();
        if (upload) {
            upload(zipFile);
        }
        LOGGER.log(Level.INFO, "The operation completed successfully");
    }

    /**
     * Upload a Zip file to AWS Glacier vault
     *
     * @param zipFile
     * @throws java.lang.Exception
     */
    public void upload(Path zipFile) throws Exception {
        GlacierTool glacier = new GlacierTool(glacierRegion, glacierVault);
        glacier.uploadArchive(glacierVault, zipFile.getFileName().toString(), zipFile);
    }

    /**
     * Parse vault inventory file and initiate archive-retrieval requests for
     * all archives in it.
     *
     * @param inventoryFile A vault inventory file, obtained with AWS CLI or
     * some other tool.
     * @throws java.lang.Exception
     */
    public void prepareDownload(Path inventoryFile) throws Exception {
        GlacierTool glacier = new GlacierTool(glacierRegion, glacierVault);
        glacier.initiateRetrievalRequests(inventoryFile);
    }

    /**
     * Download all archives in source file and save them to target folder.
     *
     * @param source A comma-separated-values file, containing job-id and
     * file-name.
     * @param target The folder to store downloaded archives.
     * @throws java.lang.Exception
     */
    public void Download(Path source, Path target) throws Exception {
        GlacierTool glacier = new GlacierTool(glacierRegion, glacierVault);
        //glacier.retrieveArchives(source);
    }

    /**
     * Extract Zip files found in source folder to target folder, then restore
     * the state of target folder to a point in time, based on snapshot file
     * found in last archive. This method will sort the list of source files and
     * start from the first.
     *
     * @param source A folder with Zip files.
     * @param target The folder to restore to.
     * @param last Name of last archive file to restore.
     * @throws java.lang.Exception
     */
    public void restore(Path source, Path target, String last) throws Exception {
        LOGGER.log(Level.INFO, "Restoring from {0} to {1}", new Object[]{source, target});
        ZipTool zip = new ZipTool();
        // Create and sort a list of zip files
        List<Path> archives = new ArrayList<>();
        DirectoryStream<Path> stream = Files.newDirectoryStream(source);
        for (Path entry : stream) {
            archives.add(entry);
        }
        Collections.sort(archives);
        // extract each file
        for (Path archive : archives) {
            LOGGER.log(Level.INFO, "Extracting {0}", archive);
            zip.extract(archive, target);
        }
        LOGGER.log(Level.INFO, "The operation completed successfully");
    }

}
