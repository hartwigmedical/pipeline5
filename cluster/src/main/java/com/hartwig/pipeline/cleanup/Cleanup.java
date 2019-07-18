package com.hartwig.pipeline.cleanup;

import java.io.IOException;

import com.google.api.services.dataproc.v1beta2.Dataproc;
import com.google.api.services.dataproc.v1beta2.model.Job;
import com.google.api.services.dataproc.v1beta2.model.ListJobsResponse;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.Run;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cleanup {

    private final static Logger LOGGER = LoggerFactory.getLogger(Cleanup.class);
    private final Storage storage;
    private final Arguments arguments;
    private final Dataproc dataproc;

    Cleanup(final Storage storage, final Arguments arguments, final Dataproc dataproc) {
        this.storage = storage;
        this.arguments = arguments;
        this.dataproc = dataproc;
    }

    public void run(SomaticRunMetadata metadata) {
        LOGGER.info("Cleaning up all transient resources on complete somatic pipeline run (runtime buckets and dataproc jobs)");
        String referenceSampleName = metadata.reference().sampleId();
        String tumorSampleName = metadata.tumor().sampleId();
        Run referenceRun = Run.from(referenceSampleName, arguments);
        deleteBucket(referenceRun.id());
        Run tumorRun = Run.from(tumorSampleName, arguments);
        deleteBucket(tumorRun.id());
        deleteBucket(Run.from(referenceSampleName, tumorSampleName, arguments).id());

        try {
            Dataproc.Projects.Regions.Jobs jobs = dataproc.projects().regions().jobs();
            ListJobsResponse execute = jobs.list(arguments.project(), arguments.region()).execute();
            for (Job job : execute.getJobs()) {
                if (job.getReference().getJobId().startsWith(referenceRun.id()) || job.getReference()
                        .getJobId()
                        .startsWith(tumorRun.id())) {
                    LOGGER.debug("Deleting complete job [{}]", job.getReference().getJobId());
                    jobs.delete(arguments.project(), arguments.region(), job.getReference().getJobId()).execute();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void deleteBucket(final String runId) {
        Bucket bucket = storage.get(runId);
        if (bucket != null) {
            LOGGER.debug("Cleaning up runtime bucket [{}]", runId);
            for (Blob blob : bucket.list().iterateAll()) {
                blob.delete();
            }
            bucket.delete();
        }
    }
}
