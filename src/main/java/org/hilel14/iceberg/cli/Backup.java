package org.hilel14.iceberg.cli;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.hilel14.iceberg.Archiver;
import org.hilel14.iceberg.Job;
import org.hilel14.iceberg.Snapshot;
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
            String job = commandLine.getOptionValue("j");
            // run
            startJob(job);
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
        // job
        option = new Option("j", "job", true, "Backup job file (found in resources/jobs)");
        option.setRequired(true);
        options.addOption(option);
        // return
        return options;
    }

    private static void startJob(String jobFile) throws Exception {
        LOGGER.log(Level.INFO, "initializing backup job from file {0}", jobFile);
        Job job = new Job(jobFile);
        Snapshot snapshot = new Snapshot();
        snapshot.load(job.getSnapshotPath());
        snapshot.getHashToPaths().clear();
        job.setSnapshot(snapshot);
        LOGGER.log(Level.INFO, "collecting files from {0} excluding pattern {1}",
                new Object[]{job.getSource(), job.getExclude()});
        Archiver archiver = new Archiver(job);
        Path archive = archiver.createArchive();
        if (job.isUploadEnabled()) {
            if (archiver.getFileCount() > 0) {
                Uploader uploader = new Uploader();
                uploader.uploadArchive(archive);
            }
            Files.delete(archive);
        }
    }

}
