package com.hartwig.pipeline.bootstrap;

import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BootstrapMain {

    private final static Logger LOGGER = LoggerFactory.getLogger(BootstrapMain.class);

    public static void main(String[] args) {

        try {
            Arguments arguments = BootstrapOptions.from(args);
            LOGGER.info("Arguments [{}]", arguments);
            try {
                BootstrapProvider.from(arguments).get().run();
            } catch (Exception e) {
                LOGGER.error("An unexpected issue arose while running the bootstrap. See the attached exception for more details.", e);
                System.exit(1);
            }
            LOGGER.info("Bootstrap completed successfully");
        } catch (ParseException e) {
            LOGGER.info("Exiting due to incorrect arguments");
        }
    }
}
