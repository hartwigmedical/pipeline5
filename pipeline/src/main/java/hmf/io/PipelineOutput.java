package hmf.io;

public enum PipelineOutput {

    UNMAPPED("bam"),
    ALIGNED("bam"),
    SORTED("bam"),
    DUPLICATE_MARKED("bam");

    private final String extension;

    PipelineOutput(final String extension) {
        this.extension = extension;
    }

    public String getExtension() {
        return extension;
    }
}
