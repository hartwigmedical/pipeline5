package com.hartwig.pipeline.calling.structural.gridss.command;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.execution.vm.Bash;

public class SoftClipsToSplitReads extends GridssCommand {

    private static final List<String> JVM_ARGUMENTS = ImmutableList.of("-Dgridss.async.buffersize=16", "-Dgridss.output_to_temp_file=true");

    public SoftClipsToSplitReads(final String inputBam, final String referenceGenome, final String outputBam) {
        super("gridss.SoftClipsToSplitReads",
                "8G",
                JVM_ARGUMENTS,
                "REFERENCE_SEQUENCE=" + referenceGenome,
                "I=" + inputBam,
                "O=" + outputBam,
                "REALIGN_EXISTING_SPLIT_READS=true",
                "REALIGN_ENTIRE_READ=true",
                "READJUST_PRIMARY_ALIGNMENT_POSITION=true",
                "WRITE_OA=false",
                "WORKER_THREADS=" + Bash.allCpus());
    }
}