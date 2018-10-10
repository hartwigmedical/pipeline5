package com.hartwig.pipeline;

import java.io.IOException;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.metrics.Metric;
import com.hartwig.pipeline.metrics.Monitor;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import htsjdk.samtools.BAMIndexer;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;

public class IndexBam {

    private final Logger LOGGER = LoggerFactory.getLogger(IndexBam.class);
    private final FileSystem fileSystem;
    private final String bamFolder;
    private final Monitor monitor;

    public IndexBam(final FileSystem fileSystem, final String bamFolder, final Monitor monitor) {
        this.fileSystem = fileSystem;
        this.bamFolder = bamFolder;
        this.monitor = monitor;
    }

    public void execute(Sample sample) throws IOException {
        SamReaderFactory samReaderFactory = SamReaderFactory.make();
        samReaderFactory.setOption(SamReaderFactory.Option.INCLUDE_SOURCE_IN_RECORDS, true);
        long startTime = System.currentTimeMillis();
        String bamFileLocation = String.format("/%s/%s.bam", bamFolder, sample.name()).substring(1);
        LOGGER.info("Indexing BAM file at [{}]", bamFileLocation);
        SamReader reader = samReaderFactory.open(SamInputResource.of(fileSystem.open(new Path(bamFileLocation))));
        String baiFileLocation = bamFileLocation + ".bai";
        BAMIndexer indexer = new BAMIndexer(fileSystem.create(new Path(baiFileLocation)), reader.getFileHeader());
        for (SAMRecord samRecord : reader) {
            indexer.processAlignment(samRecord);
        }
        indexer.finish();
        LOGGER.info("Indexing complete. BAI file can be found at [{}]", baiFileLocation);
        long endTime = System.currentTimeMillis();
        monitor.update(Metric.spentTime("BAI_CREATED", endTime - startTime));
    }
}
