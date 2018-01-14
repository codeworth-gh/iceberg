package org.hilel14.iceberg.model;

import java.nio.file.Path;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 *
 * @author hilel14
 */
public class Job {

    static final Logger LOGGER = Logger.getLogger(Job.class.getName());

    private String id;
    private String description;
    private Path sourceFolder;
    private Pattern excludeFilter;
    private boolean targetEnabled;
    private String glacierRegion;
    private String glacierVault;

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("id = ").append(id).append(", ");
        b.append("description = ").append(description).append(", ");
        b.append("source folder = ").append(sourceFolder).append(", ");
        b.append("exclude filter = ").append(excludeFilter).append(", ");
        b.append("target enabled = ").append(targetEnabled).append(", ");
        b.append("glacier region = ").append(glacierRegion).append(", ");
        b.append("glacier vault = ").append(glacierVault);
        return b.toString();
    }

    /**
     * @return the id
     */
    public String getId() {
        return id;
    }

    /**
     * @param id the id to set
     */
    public void setId(String id) {
        this.id = id;
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
     * @return the sourceFolder
     */
    public Path getSourceFolder() {
        return sourceFolder;
    }

    /**
     * @param sourceFolder the sourceFolder to set
     */
    public void setSourceFolder(Path sourceFolder) {
        this.sourceFolder = sourceFolder;
    }

    /**
     * @return the excludeFilter
     */
    public Pattern getExcludeFilter() {
        return excludeFilter;
    }

    /**
     * @param excludeFilter the excludeFilter to set
     */
    public void setExcludeFilter(Pattern excludeFilter) {
        this.excludeFilter = excludeFilter;
    }

    /**
     * @return the targetEnabled
     */
    public boolean isTargetEnabled() {
        return targetEnabled;
    }

    /**
     * @param targetEnabled the targetEnabled to set
     */
    public void setTargetEnabled(boolean targetEnabled) {
        this.targetEnabled = targetEnabled;
    }

    /**
     * @return the glacierRegion
     */
    public String getGlacierRegion() {
        return glacierRegion;
    }

    /**
     * @param glacierRegion the glacierRegion to set
     */
    public void setGlacierRegion(String glacierRegion) {
        this.glacierRegion = glacierRegion;
    }

    /**
     * @return the glacierVault
     */
    public String getGlacierVault() {
        return glacierVault;
    }

    /**
     * @param glacierVault the glacierVault to set
     */
    public void setGlacierVault(String glacierVault) {
        this.glacierVault = glacierVault;
    }

}
