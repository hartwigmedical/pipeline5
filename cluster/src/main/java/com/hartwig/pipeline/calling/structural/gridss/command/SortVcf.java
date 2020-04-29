package com.hartwig.pipeline.calling.structural.gridss.command;

public class SortVcf extends GridssCommand {

    public SortVcf(final String withBealn, final String annotatedBealn, final String outputPath) {
        super("picard.cmdline.PicardCommandLine SortVcf", "32G", "I=" + withBealn, "I=" + annotatedBealn, "O=" + outputPath);
    }
}
