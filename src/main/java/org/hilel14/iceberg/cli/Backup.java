package org.hilel14.iceberg.cli;

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
            Archiver archiver = new Archiver(job);
            Path archive = archiver.createArchive();
            Uploader uploader = new Uploader();
        } catch (ParseException ex) {
            System.out.println(ex.getMessage());
            new HelpFormatter().printHelp(Backup.class.getName(), options);
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

}
