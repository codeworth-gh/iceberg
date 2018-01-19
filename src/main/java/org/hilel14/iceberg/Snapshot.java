package org.hilel14.iceberg;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

    private void parse(Path source) throws Exception {
        try (
                FileInputStream in = new FileInputStream(source.toFile());
                JsonParser parser = new JsonFactory().createParser(in)) {
            JsonNode rootNode = null;
            description = rootNode.path("description").asText();
            date = rootNode.path("date").asText();
            Iterator<JsonNode> elements = rootNode.path("elements").elements();
            while (elements.hasNext()) {
                JsonNode element = elements.next();
                String hash = element.path("hash").asText();
                Set<String> paths = new HashSet<>();
                Iterator<JsonNode> pathsNode = element.path("paths").elements();
                while (pathsNode.hasNext()) {
                    paths.add(pathsNode.next().asText());
                }
                map.put(hash, paths);
            }
        }
    }

    private void save(Path target) throws IOException {
        LOGGER.log(Level.CONFIG, "saving snapshot to file {0}", target);
        try (
                FileOutputStream out = new FileOutputStream(target.toFile(), false);
                JsonGenerator generator = new JsonFactory().createGenerator(out)) {
            generator.setPrettyPrinter(new DefaultPrettyPrinter());
            generator.writeStartObject();
            generator.writeStringField("description", "Iceberg snapshot");
            generator.writeStringField("date", new Date().toString());
            generator.writeArrayFieldStart("elements");
            for (String hash : map.keySet()) {
                generator.writeStartObject();
                generator.writeStringField("hash", hash);
                generator.writeArrayFieldStart("paths");
                for (String path : map.get(hash)) {
                    generator.writeString(path);
                }
                generator.writeEndArray();
                generator.writeEndObject();
            }
            generator.writeEndArray();
            generator.writeEndObject(); //closing root object
            generator.flush();
        }
    }

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
