package com.hartwig.pipeline;

import java.io.IOException;

import com.hartwig.patient.Sample;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import htsjdk.samtools.BAMIndexer;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;

public class IndexBam {

    private final Logger LOGGER = LoggerFactory.getLogger(IndexBam.class);
    private final FileSystem fileSystem;
    private final String bamFolder;

    public IndexBam(final FileSystem fileSystem, final String bamFolder) {
        this.fileSystem = fileSystem;
        this.bamFolder = bamFolder;
    }

    public void execute(Sample sample) throws IOException {
        SamReaderFactory samReaderFactory = SamReaderFactory.make();
        String bamFileLocation = String.format("/%s/%s.bam", bamFolder, sample.name()).substring(1);
        LOGGER.info("Indexing BAM file at [{}]", bamFileLocation);
        SamReader reader = samReaderFactory.open(SamInputResource.of(fileSystem.open(new Path(bamFileLocation))));
        String baiFileLocation = bamFileLocation + ".bai";
        BAMIndexer indexer = new BAMIndexer(fileSystem.create(new Path(baiFileLocation)), reader.getFileHeader());
        indexer.finish();
        LOGGER.info("Indexing complete. BAI file can be found at [{}]", baiFileLocation);
    }
}
