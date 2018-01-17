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
public class Restore {

    static final Logger LOGGER = Logger.getLogger(Restore.class.getName());

    public static void main(String[] args) {

        Options options = addOptions();
        try {
            // Parse the command line arguments
            CommandLine commandLine = new DefaultParser().parse(options, args);
            Path inFolder = Paths.get(commandLine.getOptionValue("i"));
            Path outFolder = Paths.get(commandLine.getOptionValue("o"));
            // start job workflow
            Workflow workflow = new Workflow();
            workflow.restore(inFolder, outFolder, null);
        } catch (ParseException ex) {
            System.out.println(ex.getMessage());
            new HelpFormatter().printHelp("java [jvm options] " + Restore.class.getName() + " [iceberg options]", options);
            System.exit(1);
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, null, ex);
            System.exit(2);
        }
    }

    static Options addOptions() {
        Options options = new Options();
        Option option;
        // in folder
        option = new Option("i", "in-folder", true, "A folder with zip files");
        option.setRequired(true);
        options.addOption(option);
        // out folder
        option = new Option("o", "out-folder", true, "A folder to store extracted files");
        option.setRequired(true);
        options.addOption(option);
        // return
        return options;
    }
}
