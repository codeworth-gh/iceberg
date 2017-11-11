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
    private final Path snapshot;
    private final Path source;
    private final Pattern exclude;
    private final Pattern include;
    private final String region;
    private final String vault;

    public Job(String file) throws IOException {
        InputStream in = Job.class.getResourceAsStream("/jobs/" + file);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(in);
        // general
        description = rootNode.path("description").asText();
        snapshot = Paths.get(rootNode.path("snapshot").asText());
        // source
        JsonNode sourceNode = rootNode.path("source");
        source = Paths.get(sourceNode.path("path").asText());
        exclude = Pattern.compile(sourceNode.path("exclude").asText());
        include = Pattern.compile(sourceNode.path("include").asText());
        // target
        JsonNode targetNode = rootNode.path("target");
        region = targetNode.path("region").asText();
        vault = targetNode.path("vault").asText();
    }

    /**
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    /**
     * @return the snapshot
     */
    public Path getSnapshot() {
        return snapshot;
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

}
