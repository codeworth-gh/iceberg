package org.hilel14.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 *
 * @author hilel14
 */
public class Job {

    static final Logger LOGGER = Logger.getLogger(Job.class.getName());
    private final String description;
    private final Path snapshotPath;
    private Snapshot snapshot;
    private final Path source;
    private final Pattern exclude;
    private final boolean uploadEnabled;
    private final String region;
    private final String vault;

    public Job(String file) throws IOException {
        InputStream in = Job.class.getResourceAsStream("/jobs/" + file);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(in);
        JsonNode node;
        // general
        description = rootNode.path("description").asText();
        snapshotPath = Paths.get(rootNode.path("snapshot").asText());
        // source
        node = rootNode.path("source");
        source = Paths.get(node.path("path").asText());
        exclude = Pattern.compile(node.path("exclude").asText());
        // target (glacier)
        node = rootNode.path("target");
        uploadEnabled = Boolean.parseBoolean(node.path("enabled").asText());
        region = node.path("region").asText();
        vault = node.path("vault").asText();
        // done
        LOGGER.log(Level.INFO, "done loding properties for {0}", description);
    }

    /**
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    /**
     * @return the source
     */
    public Path getSource() {
        return source;
    }

    /**
     * @return the exclude
     */
    public Pattern getExclude() {
        return exclude;
    }

    /**
     * @return the region
     */
    public String getRegion() {
        return region;
    }

    /**
     * @return the vault
     */
    public String getVault() {
        return vault;
    }

    /**
     * @return the snapshotPath
     */
    public Path getSnapshotPath() {
        return snapshotPath;
    }

    /**
     * @return the snapshot
     */
    public Snapshot getSnapshot() {
        return snapshot;
    }

    /**
     * @param snapshot the snapshot to set
     */
    public void setSnapshot(Snapshot snapshot) {
        this.snapshot = snapshot;
    }

    /**
     * @return the uploadEnabled flag
     */
    public boolean isUploadEnabled() {
        return uploadEnabled;
    }

}
