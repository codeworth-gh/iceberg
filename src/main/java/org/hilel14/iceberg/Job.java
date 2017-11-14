package org.hilel14.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    private final Pattern include;
    private final Path localTarget;
    private final boolean localTargetEnabled;
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
        include = Pattern.compile(node.path("include").asText());
        // local target
        node = rootNode.path("target").path("local");
        localTarget = Paths.get(node.path("path").asText());
        localTargetEnabled = Boolean.parseBoolean(node.path("enabled").asText());
        // glacier target
        node = rootNode.path("target").path("glacier");
        region = node.path("region").asText();
        vault = node.path("vault").asText();
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
     * @return the include
     */
    public Pattern getInclude() {
        return include;
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
     * @return the localTarget
     */
    public Path getLocalTarget() {
        return localTarget;
    }

    /**
     * @return the localTargetEnabled
     */
    public boolean isLocalTargetEnabled() {
        return localTargetEnabled;
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

}
