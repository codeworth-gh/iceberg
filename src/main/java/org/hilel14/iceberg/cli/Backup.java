package org.hilel14.iceberg.cli;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.hilel14.iceberg.Workflow;

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
            String jobName = commandLine.getOptionValue("j");
            boolean upload = commandLine.hasOption("u");
            // start job workflow
            Workflow workflow = new Workflow(jobName);
            workflow.createArchive(upload);
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
        // job name
        option = new Option("j", "job-name", true, "Name of job properties file found in resources folder");
        option.setRequired(true);
        options.addOption(option);
        // upload flag
        option = new Option("u", "upload", false, "Upload archive to Glacier? Optional");
        option.setRequired(false);
        options.addOption(option);
        // return
        return options;
    }

}
