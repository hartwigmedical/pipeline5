package com.hartwig.pipeline.smoke;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;

public class ManifestAssert extends AbstractAssert<ManifestAssert, List<String>> {
    private final String referenceName;
    private final String tumorName;

    private ManifestAssert(final List<String> linesFromManifest, String referenceName, String tumorName) {
        super(linesFromManifest, ManifestAssert.class);
        this.referenceName = referenceName;
        this.tumorName = tumorName;
    }

    public static ManifestAssert assertThat(List<String> actualFiles, String referenceName, String tumorName) {
        return new ManifestAssert(actualFiles, referenceName, tumorName);
    }

    ManifestAssert hasTheseFiles(String pathToLocalCopyOfManifest, String ignoreManifestPrefix) {
        try {
            ArrayList<String> expectedFiles = new ArrayList<>(FileUtils.readLines(new File(pathToLocalCopyOfManifest)));
            Assertions.assertThat(sanitise(sliceOutFilenames(actual, 1, "")))
                    .containsOnlyElementsOf(sliceOutFilenames(expectedFiles, 2, ignoreManifestPrefix));
            return this;
        } catch (IOException ioe) {
            throw new RuntimeException(format("Could not open local copy of manifest '%s'", pathToLocalCopyOfManifest));
        }
    }

    private List<String> sanitise(List<String> filenames) {
        return filenames.stream().filter(name -> {
            String trimmed = name.trim();
            return !(trimmed.equals("STAGED") || trimmed.equals(format("%s/run.log", referenceName)) || trimmed.equals(format("%s/run.log",
                    tumorName)));
        }).collect(toList());
    }

    private List<String> sliceOutFilenames(List<String> filenames, int filenameIndex, String prefixToRemoveFromFilenames) {
        return filenames.stream()
                .map(s -> s.trim().split(" +")[filenameIndex].replaceAll("^" + prefixToRemoveFromFilenames, ""))
                .collect(toList());
    }
}
