package org.hilel14.iceberg;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    private final Map<String, Set<Path>> map = new HashMap<>();

    private void parse(JsonNode rootNode) {
        Iterator<JsonNode> elements = rootNode.path("elements").elements();
        while (elements.hasNext()) {
            JsonNode element = elements.next();
            String hash = element.path("hash").asText();
            Set<Path> paths = new HashSet<>();
            Iterator<JsonNode> pathsNode = element.path("paths").elements();
            while (pathsNode.hasNext()) {
                paths.add(Paths.get(pathsNode.next().asText()));
            }
            map.put(hash, paths);
        }
    }

    public void save(Path target) throws IOException {
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
                for (Path path : map.get(hash)) {
                    generator.writeString(path.toString());
                }
                generator.writeEndArray();
                generator.writeEndObject();
            }
            generator.writeEndArray();
            generator.writeEndObject(); //closing root object
            generator.flush();
        }
    }

    public void add(String hash, Path path) {
        Set<Path> paths = map.containsKey(hash) ? map.get(hash) : new HashSet<>();
        paths.add(path);
        map.put(hash, paths);
    }

    /**
     * @return the hash-to-paths map
     */
    public Map<String, Set<Path>> getMap() {
        return map;
    }

}
