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
            Path inventoryFile = Paths.get(commandLine.getOptionValue("i"));
            Path targetFolder = Paths.get(commandLine.getOptionValue("t"));
            String glacierRegion = commandLine.getOptionValue("r");
            String glacierVault = commandLine.getOptionValue("v");
            // run
            //Downloader downloader = new Downloader(glacierRegion);
            //downloader.download(inventoryFile, targetFolder, glacierVault);
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
        // inventory file
        option = new Option("i", "inventory", true, "Path to AWS Glacier vault inventory json file");
        option.setRequired(true);
        options.addOption(option);
        // tareget folder
        option = new Option("t", "target", true, "Path to an empty folder to hold restored data");
        option.setRequired(true);
        options.addOption(option);
        // glacier region
        option = new Option("r", "region", true, "Name of Glacier region");
        option.setRequired(true);
        options.addOption(option);
        // glacier vault
        option = new Option("v", "vault", true, "Name of Glacier vault");
        option.setRequired(true);
        options.addOption(option);
        // return
        return options;
    }

}
