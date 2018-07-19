package com.hartwig.io;

import static java.lang.String.format;

import java.io.File;

import com.hartwig.patient.FileSystemEntity;
import com.hartwig.patient.FileSystemVisitor;
import com.hartwig.patient.Lane;
import com.hartwig.patient.Patient;
import com.hartwig.patient.Sample;

public class OutputFile implements FileSystemVisitor {

    public static final String RESULTS_DIRECTORY = format("%sresults/", workingDirectory());
    private final OutputType output;
    private String path;
    private String file;

    private OutputFile(final OutputType output) {
        this.output = output;
    }

    @Override
    public void visit(final Patient patient) {
        deny("Take it easy, the patient file sample structure is not yet implemented.");
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

    public static OutputFile of(OutputType output, FileSystemEntity fileSystemEntity) {
        OutputFile outputFile = new OutputFile(output);
        fileSystemEntity.accept(outputFile);
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
        return directory.equals(File.separator) ? directory : directory + File.separator;
    }
}
