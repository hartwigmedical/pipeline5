package com.hartwig.pipeline.janitor;

import java.util.Optional;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JanitorOptions {

    private static final Logger LOGGER = LoggerFactory.getLogger(JanitorOptions.class);

    static final String DEFAULT_PRIVATE_KEY_PATH = "/secrets/janitor-key.json";
    static final String PRIVATE_KEY_FLAG = "k";
    static final String PROJECT_FLAG = "project";
    static final String REGION_FLAG = "region";
    static final String DEFAULT_REGION = "europe-west4";
    static final String DEFAULT_PROJECT = "hmf-pipeline-development";
    static final String DEFAULT_INTERVAL_SECONDS = "300";
    static final String INTERVAL_FLAG = "interval";

    private static Options options() {
        return new Options().addOption(privateKeyFlag()).addOption(project()).addOption(region()).addOption(intervalInSeconds());
    }

    private static Option intervalInSeconds() {
        return optionWithArgAndDefault(INTERVAL_FLAG,
                INTERVAL_FLAG,
                "Time interval on which the janitor will run, in seconds",
                DEFAULT_INTERVAL_SECONDS);
    }

    private static Option privateKeyFlag() {
        return optionWithArgAndDefault(PRIVATE_KEY_FLAG,
                "private_key_path",
                "Fully qualified path to the private key for the service account used" + "for all Google Cloud operations",
                DEFAULT_PRIVATE_KEY_PATH);
    }

    private static Option region() {
        return optionWithArgAndDefault(REGION_FLAG, "region", "The region in which to create the cluster.", DEFAULT_REGION);
    }

    private static Option project() {
        return optionWithArgAndDefault(PROJECT_FLAG, "project", "The Google project for which to create the cluster.", DEFAULT_PROJECT);
    }

    private static String handleDashesInRegion(final CommandLine commandLine) {
        if (commandLine.hasOption(REGION_FLAG)) {
            return commandLine.getOptionValue(REGION_FLAG);
        }
        return DEFAULT_REGION;
    }

    static Optional<Arguments> from(String[] args) {
        try {
            DefaultParser defaultParser = new DefaultParser();
            CommandLine commandLine = defaultParser.parse(options(), args);
            return Optional.of(Arguments.builder()
                    .privateKeyPath(commandLine.getOptionValue(PRIVATE_KEY_FLAG, DEFAULT_PRIVATE_KEY_PATH))
                    .project(commandLine.getOptionValue(PROJECT_FLAG, DEFAULT_PROJECT))
                    .region(handleDashesInRegion(commandLine))
                    .intervalInSeconds(intervalInSeconds(commandLine))
                    .build());
        } catch (ParseException e) {
            LOGGER.error("Could not parse command line args", e);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("janitor", options());
            return Optional.empty();
        }
    }

    private static int intervalInSeconds(final CommandLine commandLine) {
        return Integer.parseInt(commandLine.getOptionValue(INTERVAL_FLAG, DEFAULT_INTERVAL_SECONDS));
    }

    private static Option optionWithArgAndDefault(final String option, final String name, final String description,
            final String defaultValue) {
        return optionWithArg(option, name, description + " Default is " + (defaultValue.isEmpty() ? "empty" : defaultValue));
    }

    private static Option optionWithArg(final String option, final String name, final String description) {
        return Option.builder(option).hasArg().argName(name).desc(description).build();
    }
}
