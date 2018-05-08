package hmf.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Trace {

    private final Logger logger;
    private final String messagePrefix;
    private static final String TEMPLATE = "{} : {}";

    private Trace(final Logger logger, final String messagePrefix) {
        this.logger = logger;
        this.messagePrefix = messagePrefix;
    }

    public static Trace of(Class clazz, String message) {
        return new Trace(LoggerFactory.getLogger(clazz), message);
    }

    void start() {
        logger.info(TEMPLATE, messagePrefix, "Started");
    }

    void finish() {
        logger.info(TEMPLATE, messagePrefix, "Completed");
    }
}
