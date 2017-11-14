package org.hilel14.iceberg;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
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
    private final Map<String, Set<Path>> hashToPaths = new HashMap<>();
    private Set<String> fileHashes;

    public void load(Path path) throws IOException {
        if (Files.exists(path)) {
            LOGGER.log(Level.INFO, "loading snapshot from file {0}", path);
            try (InputStream in = new FileInputStream(path.toFile())) {
                ObjectMapper objectMapper = new ObjectMapper();
                JsonNode rootNode = objectMapper.readTree(in);
                parse(rootNode);
            }
        } else {
            LOGGER.log(Level.INFO, "snapshot file {0} not found, assuming full backup", path);
        }
        fileHashes = new HashSet<>(hashToPaths.keySet());
    }

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
            hashToPaths.put(hash, paths);
        }
    }

    public void save(Path target) throws IOException {
        LOGGER.log(Level.INFO, "saving snapshot to file {0}", target);
        try (
                FileOutputStream out = new FileOutputStream(target.toFile(), false);
                JsonGenerator generator = new JsonFactory().createGenerator(out)) {
            generator.setPrettyPrinter(new DefaultPrettyPrinter());
            generator.writeStartObject();
            generator.writeStringField("description", "Iceberg snapshot");
            generator.writeStringField("date", new Date().toString());
            generator.writeArrayFieldStart("elements");
            for (String hash : getHashToPaths().keySet()) {
                generator.writeStartObject();
                generator.writeStringField("hash", hash);
                generator.writeArrayFieldStart("paths");
                for (Path path : getHashToPaths().get(hash)) {
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

    /**
     * @return the hashToPaths
     */
    public Map<String, Set<Path>> getHashToPaths() {
        return hashToPaths;
    }

    /**
     * @return the fileHashes
     */
    public Set<String> getFileHashes() {
        return fileHashes;
    }

}
