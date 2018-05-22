package hmf.pipeline;

import static java.lang.String.format;

import hmf.sample.Lane;

public enum PipelineOutput {

    UNMAPPED("bam"),
    ALIGNED("bam"),
    SORTED("bam");

    private final String extension;
    private static final String RESULTS_DIRECTORY = format("%s/results/", workingDirectory());

    PipelineOutput(final String extension) {
        this.extension = extension;
    }

    public String path(Lane lane) {
        return format("%s%s", RESULTS_DIRECTORY, file(lane));
    }

    public String file(Lane lane) {
        return format("%s_L00%s_%s.%s", lane.sample().name(), lane.index(), toString().toLowerCase(), extension);
    }

    private static String workingDirectory() {
        return System.getProperty("user.dir");
    }
}
