package org.hilel14.iceberg.cli;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.hilel14.iceberg.Archiver;
import org.hilel14.iceberg.Uploader;

/**
 *
 * @author hilel14
 */
public class Backup {

    static final Logger LOGGER = Logger.getLogger(Backup.class.getName());

    public static void main(String[] args) {

        Options options = addOptions();
        try {
            // Parse the command line arguments
            CommandLine commandLine = new DefaultParser().parse(options, args);
            Path snapshotFile = Paths.get(commandLine.getOptionValue("s"));
            Path inputFolder = Paths.get(commandLine.getOptionValue("i"));
            Pattern excludePatten
                    = commandLine.hasOption("e")
                    ? Pattern.compile(commandLine.getOptionValue("e"))
                    : getDefaultPattern();
            boolean upload = commandLine.hasOption("u");
            // run
            startJob(snapshotFile, inputFolder, excludePatten, upload);
        } catch (ParseException ex) {
            System.out.println(ex.getMessage());
            new HelpFormatter().printHelp("java [options] " + Backup.class.getName() + " [args]", options);
            System.exit(1);
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, null, ex);
            System.exit(2);
        }
    }

    static Options addOptions() {
        Options options = new Options();
        Option option;
        // snapshot file
        option = new Option("s", "snapshot-file", true, "Path to snapshot file");
        option.setRequired(true);
        options.addOption(option);
        // input folder
        option = new Option("i", "input-folder", true, "Path to a folder with files to backup");
        option.setRequired(true);
        options.addOption(option);
        // exclude pattern
        option = new Option("e", "exclude-pattern", true, "Java regex for file names you want to exclude from the backup. Optional (default from properties file)");
        option.setRequired(false);
        options.addOption(option);
        // upload flag
        option = new Option("u", "upload", false, "Upload archive to Glacier vault. Optional flag");
        option.setRequired(false);
        options.addOption(option);
        // return
        return options;
    }

    private static void startJob(
            Path snapshotFile,
            Path inputFolder,
            Pattern excludePattern,
            boolean upload)
            throws Exception {

        Archiver archiver = new Archiver(
                snapshotFile, inputFolder, excludePattern);
        Path archive = archiver.createArchive();
        if (upload) {
            if (archiver.getFileCount() > 0) {
                Uploader uploader = new Uploader();
                uploader.uploadArchive(archive);
            }
            Files.delete(archive);
        }
    }

    private static Pattern getDefaultPattern() throws IOException {
        Properties properties = new Properties();
        properties.load(Backup.class.getResourceAsStream("/iceberg.properties"));
        Pattern pattern = Pattern.compile(properties.getProperty("exclude.pattern"));
        return pattern;
    }

}
