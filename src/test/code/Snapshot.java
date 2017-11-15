package org.hilel14.iceberg;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
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
    private Map<String, Set<Path>> hashToPaths = new HashMap<>();
    private Set<String> fileHashes = new HashSet<>();

    public void newMap() {
        hashToPaths = new HashMap<>();
    }

    public void load(Path path) throws IOException {
        if (Files.exists(path)) {
            LOGGER.log(Level.INFO, "loading snapshot from file {0}", path);
            try (InputStream in = new FileInputStream(path.toFile());
                    JsonParser parser = new JsonFactory().createParser(in)) {
                parse(parser);
                fileHashes = hashToPaths.keySet();
            }
        } else {
            LOGGER.log(Level.INFO, "snapshot file {0} not found, assuming full backup", path);
        }
    }

    private void parse(JsonParser parser) throws IOException {
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            switch (String.valueOf(parser.getCurrentName())) {
                case "description":
                    parser.nextToken();
                    LOGGER.log(Level.INFO, "description: {0}", parser.getText());
                    break;
                case "date":
                    parser.nextToken();
                    LOGGER.log(Level.INFO, "date: {0}", parser.getText());
                    break;
                case "elements":
                    parseElements(parser);
                    break;
                default:
                    LOGGER.log(Level.WARNING, "unknown current name: {0}", parser.getCurrentName());
                    break;
            }
        }
    }

    private void parseElements(JsonParser parser) throws IOException {
        String hash = null;
        Set<Path> paths = null;
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            switch (String.valueOf(parser.getCurrentName())) {
                case "hash":
                    parser.nextToken();
                    hash = parser.getText();
                    paths = new HashSet<>();
                    break;
                case "paths":
                    parser.nextToken();
                    while (parser.nextToken() != JsonToken.END_ARRAY) {
                        paths.add(Paths.get(parser.getText()));
                    }
                    hashToPaths.put(hash, paths);
                    break;
                default:
                    LOGGER.log(Level.WARNING, "unknown current name: {0}", parser.getCurrentName());
                    break;
            }
        }
    }

    private void parse2(JsonParser parser) throws IOException {
        String hash = null;
        Set<Path> paths = null;
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            if ("description".equals(parser.getCurrentName())) {
                parser.nextToken();
                LOGGER.log(Level.INFO, "description: {0}", parser.getText());
            } else if ("date".equals(parser.getCurrentName())) {
                parser.nextToken();
                LOGGER.log(Level.INFO, "date: {0}", parser.getText());
            } else if ("elements".equals(parser.getCurrentName())) {
                parser.nextToken();
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    parser.nextToken();
                    if ("hash".equals(parser.getCurrentName())) {
                        hash = parser.getText();
                        paths = new HashSet<>();
                    } else if ("paths".equals(parser.getCurrentName())) {
                        parser.nextToken();
                        while (parser.nextToken() != JsonToken.END_ARRAY) {
                            paths.add(Paths.get(parser.getText()));
                        }
                        hashToPaths.put(hash, paths);
                    }
                }
            }
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
