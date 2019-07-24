package com.hartwig.bam.after;

import java.io.IOException;

import com.hartwig.bam.runtime.Processes;
import com.hartwig.patient.Sample;

class SamtoolsSortAndIndex implements SortAndIndexer {
    @Override
    public void execute(final Sample sample, final String workingDirectory) throws IOException, InterruptedException {
        Processes.run(new ProcessBuilder("samtools",
                "sort",
                Bams.name(sample, workingDirectory, Bams.UNSORTED),
                "-o",
                Bams.name(sample, workingDirectory, Bams.SORTED)));
        Processes.run(new ProcessBuilder("samtools", "index", Bams.name(sample, workingDirectory, Bams.SORTED)));
    }
}
