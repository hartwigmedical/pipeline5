package com.hartwig.pipeline.janitor;

import com.google.api.services.dataproc.model.Cluster;
import com.google.api.services.dataproc.model.Job;

class ResourceState {

    private static final String RUNNING = "RUNNING";

    static boolean running(Cluster cluster) {
        return cluster.getStatus().getState().toUpperCase().equals(RUNNING);
    }

    static boolean running(Job job) {
        return job.getStatus().getState().equals(RUNNING);
    }
}
