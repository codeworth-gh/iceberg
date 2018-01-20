package org.hilel14.iceberg;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author hilel14
 */
public class Snapshot {

    static final Logger LOGGER = Logger.getLogger(Snapshot.class.getName());

    @JsonProperty("elements")
    private Map<String, Set<String>> map = new HashMap<>();

    @JsonProperty("description")
    private String description = "Iceberg snapshot";

    @JsonProperty("date")
    private String date = new Date().toString();

    public void addPath(String hash, Path path) {
        Set<String> paths = map.containsKey(hash) ? map.get(hash) : new HashSet<>();
        paths.add(path.toString());
        map.put(hash, paths);
    }

    private boolean containsPath(String path) {
        return map.values().stream().anyMatch((set) -> (set.contains(path)));
    }

    /**
     * Restore state of target folder
     *
     * @param target A folder with files extracted from archives
     * @throws java.lang.Exception
     */
    public void restore(Path target) throws Exception {
        // delete files and empty folders in target folder and not in snapshot
        LOGGER.log(Level.INFO, "Deleting old files from {0}", target);
        Files.walkFileTree(target, new Cleaner(target));
    }

    class Cleaner extends SimpleFileVisitor<Path> {

        Path baseFolder;

        public Cleaner(Path baseFolder) {
            this.baseFolder = baseFolder;
        }

        @Override
        public FileVisitResult visitFile(Path path, BasicFileAttributes attrs)
                throws IOException {
            Path relative = baseFolder.getParent().relativize(path);
            if (!containsPath(relative.toString())) {
                LOGGER.log(Level.INFO, "Deleting {0}", relative);
                Files.delete(path);
            }
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException ex) {

            return FileVisitResult.CONTINUE;
        }
    }

    /**
     * @return the map
     */
    public Map<String, Set<String>> getMap() {
        return map;
    }

    /**
     * @param map the map to set
     */
    public void setMap(Map<String, Set<String>> map) {
        this.map = map;
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
     * @return the date
     */
    public String getDate() {
        return date;
    }

    /**
     * @param date the date to set
     */
    public void setDate(String date) {
        this.date = date;
    }

}
