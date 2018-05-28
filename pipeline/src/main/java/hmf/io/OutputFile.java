package hmf.io;

import static java.lang.String.format;

import hmf.sample.FlowCell;
import hmf.sample.HasSample;
import hmf.sample.HasSampleVisitor;
import hmf.sample.Lane;

public class OutputFile implements HasSampleVisitor {

    private static final String RESULTS_DIRECTORY = format("%s/results/", workingDirectory());
    private final PipelineOutput output;
    private String path;
    private String file;

    private OutputFile(final PipelineOutput output) {
        this.output = output;
    }

    @Override
    public void visit(final FlowCell cell) {
        path = path(cell);
        file = file(cell);
    }

    @Override
    public void visit(final Lane lane) {
        path = path(lane);
        file = file(lane);
    }

    public String path() {
        return path;
    }

    public String file() {
        return file;
    }

    public static OutputFile of(PipelineOutput output, HasSample hasSample) {
        OutputFile outputFile = new OutputFile(output);
        hasSample.accept(outputFile);
        return outputFile;
    }

    private String path(Lane lane) {
        return format("%s%s", RESULTS_DIRECTORY, file(lane));
    }

    private String path(FlowCell flowCell) {
        return format("%s%s", RESULTS_DIRECTORY, file(flowCell));
    }

    private String file(FlowCell flowCell) {
        return format("%s_%s.%s", flowCell.sample().name(), output.toString().toLowerCase(), output.getExtension());
    }

    private String file(Lane lane) {
        return format("%s_L00%s_%s.%s", lane.sample().name(), lane.index(), output.toString().toLowerCase(), output.getExtension());
    }

    private static String workingDirectory() {
        return System.getProperty("user.dir");
    }

}
