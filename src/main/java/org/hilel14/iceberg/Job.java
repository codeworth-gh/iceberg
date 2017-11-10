package org.hilel14.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.regex.Pattern;

/**
 *
 * @author hilel14
 */
public class Job {

    private String description;
    private Map<String, Path> snapshot;
    private Path source;
    private Pattern exclude;
    private Pattern include;
    private String region;
    private String vault;

    public Job() {

    }

    public Job(String file) throws IOException {
        InputStream in = Job.class.getResourceAsStream("/jobs/" + file);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(in);
        JsonNode node;
        // description
        node = rootNode.path("description");
        description = node.asText();
        // source folder
        node = rootNode.path("source").path("path");
        source = Paths.get(node.asText());
    }

    /**
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    /**
     * @param description the description to set
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * @return the snapshot
     */
    public Map<String, Path> getSnapshot() {
        return snapshot;
    }

    /**
     * @param snapshot the snapshot to set
     */
    public void setSnapshot(Map<String, Path> snapshot) {
        this.snapshot = snapshot;
    }

    /**
     * @return the source
     */
    public Path getSource() {
        return source;
    }

    /**
     * @param source the source to set
     */
    public void setSource(Path source) {
        this.source = source;
    }

    /**
     * @return the exclude
     */
    public Pattern getExclude() {
        return exclude;
    }

    /**
     * @param exclude the exclude to set
     */
    public void setExclude(Pattern exclude) {
        this.exclude = exclude;
    }

    /**
     * @return the include
     */
    public Pattern getInclude() {
        return include;
    }

    /**
     * @param include the include to set
     */
    public void setInclude(Pattern include) {
        this.include = include;
    }

    /**
     * @return the region
     */
    public String getRegion() {
        return region;
    }

    /**
     * @param region the region to set
     */
    public void setRegion(String region) {
        this.region = region;
    }

    /**
     * @return the vault
     */
    public String getVault() {
        return vault;
    }

    /**
     * @param vault the vault to set
     */
    public void setVault(String vault) {
        this.vault = vault;
    }
}
