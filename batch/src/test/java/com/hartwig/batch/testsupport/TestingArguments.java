package com.hartwig.batch.testsupport;

import com.hartwig.batch.BatchArguments;
import com.hartwig.pipeline.testsupport.Resources;

public class TestingArguments {
    public static BatchArguments defaultArgs(final String operation) {
        BatchArguments baseWithDefaults = BatchArguments.from(new String[]{operation,
                "-" + BatchArguments.PRIVATE_KEY_PATH, "irrelevant",
                "-" + BatchArguments.SERVICE_ACCOUNT_EMAIL, "irrelevant",
                "-" + BatchArguments.INPUT_FILE, Resources.testResource("batch-dispatcher/batch_descriptor.json"),
                "-" + BatchArguments.OUTPUT_BUCKET, "irrelevant"});
        return BatchArguments.builder().from(baseWithDefaults).build();
    }
}
