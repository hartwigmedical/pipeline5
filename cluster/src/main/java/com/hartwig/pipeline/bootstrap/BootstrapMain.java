package com.hartwig.pipeline.bootstrap;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BootstrapMain {

    private final static Logger LOGGER = LoggerFactory.getLogger(BootstrapMain.class);

    public static void main(String[] args) {
        Optional<Arguments> optArguments = BootstrapOptions.from(args);
        if (optArguments.isPresent()) {
            Arguments arguments = optArguments.get();
            LOGGER.info("Arguments [{}]", arguments);
            try {
                BootstrapProvider.from(arguments).get().run();
            } catch (Exception e) {
                LOGGER.error("An unexpected issue arose while running the bootstrap. See the attached exception for more details.", e);
                System.exit(1);
            }
            LOGGER.info("Bootstrap completed successfully");
        }
    }
}
