package org.hilel14.iceberg.cli;

import java.nio.file.Path;
import java.nio.file.Paths;
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
public class Download {

    static final Logger LOGGER = Logger.getLogger(Download.class.getName());

    public static void main(String[] args) {

        Options options = addOptions();
        try {
            // Parse the command line arguments
            CommandLine commandLine = new DefaultParser().parse(options, args);
            Path inputFile = Paths.get(commandLine.getOptionValue("i"));
            Path targetFolder = Paths.get(commandLine.getOptionValue("t"));
            // run
            Workflow workflow = new Workflow();
            workflow.Download(inputFile, targetFolder);
        } catch (ParseException ex) {
            System.out.println(ex.getMessage());
            new HelpFormatter().printHelp("java [jvm options] " + Download.class.getName() + " [iceberg options]", options);
            System.exit(1);
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, null, ex);
            System.exit(2);
        }
    }

    static Options addOptions() {
        Options options = new Options();
        Option option;
        // input file
        option = new Option("i", "input-file", true, "Path to csv file containing retrieval requests");
        option.setRequired(true);
        options.addOption(option);
        // target folder
        option = new Option("t", "target-folder", true, "Path a folder to save the downloaded archives");
        option.setRequired(true);
        options.addOption(option);
        // return
        return options;
    }

}
