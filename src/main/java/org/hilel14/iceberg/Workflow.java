package org.hilel14.iceberg;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
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

    public Workflow(String jobName) throws Exception {
        this.jobName = jobName;
        // load global application properties
        Properties p = new Properties();
        p.load(Workflow.class.getResourceAsStream("iceberg.properties"));
        workFolder = Paths.get(p.getProperty("work.folder"));
        // load current job properties
        p.load(Workflow.class.getResourceAsStream("jobs/" + jobName + ".properties"));
        sourceFolder = Paths.get(p.getProperty("soruce.folder"));
        excludeFilter = Pattern.compile(p.getProperty("soruce.folder"));
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
            upload();
        }
    }

    /**
     * Upload a Zip file to AWS Glacier vault
     */
    public void upload() {

    }

    /**
     * Parse vault inventory file and initiate archive-retrieval requests for
     * all archives in it.
     *
     * @param inventoryFile A vault inventory file, obtained with AWS CLI or
     * some other tool.
     */
    public void prepareDownload(Path inventoryFile) {

    }

    /**
     * Download all archives in source file and save them to target folder.
     *
     * @param source A comma-separated-values file, containing job-id and
     * file-name.
     * @param target The folder to store downloaded archives.
     */
    public void Download(Path source, Path target) {

    }

    /**
     * Extract Zip files found in source folder to target folder, then restore
     * the state of target folder to a point in time, based on snapshot file
     * found in lash archive. This method will sort the list of source files and
     * start from the first.
     *
     * @param source A folder with Zip files.
     * @param target The folder to restore to.
     * @param last Name of last archive file to restore.
     */
    public void restore(Path source, Path target, String last) {

    }

    /**
     * Extract a Zip file to a folder, replacing existing files with the same
     * path.
     *
     * @param source A Zip file
     * @param target The folder where extracted files are saved
     */
    private void extract(Path source, Path target) {

    }

}
