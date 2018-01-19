package org.hilel14.iceberg;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.file.Path;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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

    public boolean containsPath(String path) {
        for (Set<String> set : map.values()) {
            if (set.contains(path)) {
                return true;
            }
        }
        return false;
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
