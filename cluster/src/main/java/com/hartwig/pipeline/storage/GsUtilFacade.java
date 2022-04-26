package com.hartwig.pipeline.storage;

import static java.util.Arrays.stream;

public class GsUtilFacade {
    private final String cloudSdkPath;
    private final String privateKeyPath;
    private String userProject;

    public GsUtilFacade(final String cloudSdkPath, final String userProject, final String privateKeyPath) {
        this.cloudSdkPath = cloudSdkPath;
        this.userProject = userProject;
        this.privateKeyPath = privateKeyPath;
    }

    public void copy(final String source, final String destination, final GsCopyOption... copyOptions) {
        try {
            GSUtil.auth(cloudSdkPath, privateKeyPath);
            GSUtil.cp(cloudSdkPath, source, destination, userProject,
                    stream(copyOptions).map(s -> s.option).toArray(String[]::new));
        } catch (Exception e) {
            throw new RuntimeException("Copy operation failed", e);
        }
    }

    public enum GsCopyOption {
        NO_CLOBBER("-n");

        private String option;

        GsCopyOption(final String option) {
            this.option = option;
        }
    }
}
