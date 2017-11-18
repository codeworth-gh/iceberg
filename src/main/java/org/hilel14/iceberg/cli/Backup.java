package org.hilel14.iceberg.cli;

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
import org.hilel14.iceberg.Configuration;
import org.hilel14.iceberg.Job;
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
            String jobId = commandLine.getOptionValue("j");
            // run
            startJob(jobId);
        } catch (ParseException ex) {
            System.out.println(ex.getMessage());
            new HelpFormatter().printHelp("java [jvm options] " + Backup.class.getName() + " [iceberg options]", options);
            System.exit(1);
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, null, ex);
            System.exit(2);
        }
    }

    static Options addOptions() {
        Options options = new Options();
        Option option;
        // job id
        option = new Option("j", "job-id", true, "ID of a job from iceberg.conf.xml");
        option.setRequired(true);
        options.addOption(option);
        // return
        return options;
    }

    private static void startJob(String jobId) throws Exception {
        Configuration config = new Configuration();
        Job job = config.getBackupJobs().get(jobId);
        Archiver archiver = new Archiver(job, config.getWorkFolder());
        Path archive = archiver.createArchive();
        if (job.isTargetEnabled()) {
            if (archiver.getFileCount() > 0) {
                Uploader uploader = new Uploader();
                uploader.uploadArchive(archive);
            }
            Files.delete(archive);
        }
    }

}
