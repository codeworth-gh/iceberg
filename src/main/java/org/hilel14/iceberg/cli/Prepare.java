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
public class Prepare {

    static final Logger LOGGER = Logger.getLogger(Prepare.class.getName());

    public static void main(String[] args) {

        Options options = addOptions();
        try {
            // Parse the command line arguments
            CommandLine commandLine = new DefaultParser().parse(options, args);
            Path inventoryFile = Paths.get(commandLine.getOptionValue("i"));
            // run
            Workflow workflow = new Workflow();
            workflow.prepareDownload(inventoryFile);
        } catch (ParseException ex) {
            System.out.println(ex.getMessage());
            new HelpFormatter().printHelp("java [jvm options] " + Prepare.class.getName() + " [iceberg options]", options);
            System.exit(1);
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, null, ex);
            System.exit(2);
        }
    }

    static Options addOptions() {
        Options options = new Options();
        Option option;
        // inventory file
        option = new Option("i", "inventory", true, "Path to AWS Glacier vault inventory json file");
        option.setRequired(true);
        options.addOption(option);
        // return
        return options;
    }

}
