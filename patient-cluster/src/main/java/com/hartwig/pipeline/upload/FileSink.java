package com.hartwig.pipeline.upload;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import com.hartwig.patient.Sample;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSink implements BamSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSink.class);

    private FileSink() {
    }

    public static FileSink newInstance() {
        return new FileSink();
    }

    @Override
    public void save(final Sample sample, final InputStream bam, final InputStream bai) {
        saveFile(sample, bam, ".bam");
        saveFile(sample, bai, ".bam.bai");
    }

    private void saveFile(final Sample sample, final InputStream bam, final String extension) {
        File results = new File(System.getProperty("user.dir") + "/results");
        boolean dirExists = results.exists();
        if (!dirExists) {
            dirExists = results.mkdirs();
        }
        if (dirExists) {
            String fileName = results.getPath() + "/" + sample.name() + extension;
            try (FileOutputStream fileOutputStream = new FileOutputStream(fileName)) {
                IOUtils.copy(bam, fileOutputStream);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            LOGGER.info("Completed download to file [{}]", fileName);
        } else {
            LOGGER.warn("Could not create results dir [{}]. No files were downloaded", results.getPath());
        }
    }
}
