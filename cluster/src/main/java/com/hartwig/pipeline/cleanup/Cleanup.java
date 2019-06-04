package com.hartwig.pipeline.cleanup;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.alignment.Run;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cleanup {

    private final static Logger LOGGER = LoggerFactory.getLogger(Logger.class);
    private final Storage storage;
    private final Arguments arguments;

    public Cleanup(final Storage storage, final Arguments arguments) {
        this.storage = storage;
        this.arguments = arguments;
    }

    public void run(AlignmentPair pair) {
        String referenceSampleName = pair.reference().sample().name();
        String tumorSampleName = pair.tumor().sample().name();
        deleteBucket(Run.from(referenceSampleName, arguments).id());
        deleteBucket(Run.from(tumorSampleName, arguments).id());
        deleteBucket(Run.from(referenceSampleName, tumorSampleName, arguments).id());
    }

    private void deleteBucket(final String runId) {
        Bucket bucket = storage.get(runId);
        if (bucket != null) {
            LOGGER.info("Cleaning up runtime bucket [{}]", runId);
            for (Blob blob : bucket.list().iterateAll()) {
                blob.delete();
            }
            bucket.delete();
        }
    }
}
