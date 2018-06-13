package hmf.io;

import static java.lang.String.format;

import java.io.File;

import hmf.patient.FileSystemEntity;
import hmf.patient.FileSystemVisitor;
import hmf.patient.Lane;
import hmf.patient.Patient;
import hmf.patient.Sample;

public class OutputFile implements FileSystemVisitor {

    private static final String RESULTS_DIRECTORY = format("%sresults/", workingDirectory());
    private final OutputType output;
    private String path;
    private String file;

    private OutputFile(final OutputType output) {
        this.output = output;
    }

    @Override
    public void visit(final Patient patient) {
        deny("Take it easy, the patient file entity structure is not yet implemented.");
    }

    @Override
    public void visit(final Sample sample) {
        path = path(sample);
        file = file(sample);
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

    public static OutputFile of(OutputType output, FileSystemEntity hasSample) {
        OutputFile outputFile = new OutputFile(output);
        hasSample.accept(outputFile);
        return outputFile;
    }

    private String path(Lane lane) {
        return format("%s%s", RESULTS_DIRECTORY, file(lane));
    }

    private String path(Sample sample) {
        return format("%s%s", RESULTS_DIRECTORY, file(sample));
    }

    private String file(Sample sample) {
        return format("%s_%s.%s", sample.name(), output.toString().toLowerCase(), output.getExtension());
    }

    private String file(Lane lane) {
        return format("%s_%s.%s", lane.name(), output.toString().toLowerCase(), output.getExtension());
    }

    private static String workingDirectory() {
        String directory = System.getProperty("user.dir");
        return directory.isEmpty() ? "" : directory + File.separator;
    }
}
