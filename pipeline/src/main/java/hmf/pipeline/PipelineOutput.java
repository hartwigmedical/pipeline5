package hmf.pipeline;

import static java.lang.String.format;

public enum PipelineOutput {

    UNMAPPED("bam"),
    ALIGNED("bam"),
    SORTED("bam");

    private final String extension;
    private static final String RESULTS_DIRECTORY = format("%s/results/", workingDirectory());

    PipelineOutput(final String extension) {
        this.extension = extension;
    }

    public String path(String sampleName) {
        return format("%s%s", RESULTS_DIRECTORY, file(sampleName));
    }

    public String file(String sampleName) {
        return format("%s_%s.%s", sampleName, toString().toLowerCase(), extension);
    }

    private static String workingDirectory() {
        return System.getProperty("user.dir");
    }
}
