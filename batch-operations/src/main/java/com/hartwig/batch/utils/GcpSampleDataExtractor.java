package com.hartwig.batch.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.logging.Logger;

import com.google.common.collect.Lists;
import com.hartwig.batch.api.RemoteLocationsApi;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.jetbrains.annotations.NotNull;

public class GcpSampleDataExtractor
{
    private static final Logger LOGGER = Logger.getLogger(GcpSampleDataExtractor.class.getName());

    private final List<String> mSampleIds;

    private BufferedWriter mWriter;

    public static final String SAMPLE_IDS_FILE = "sample_ids_file";
    public static final String OUTPUT_FILE = "output_file";

    public GcpSampleDataExtractor(final CommandLine cmd)
    {
        mSampleIds = Lists.newArrayList();
        loadSampleIds(cmd.getOptionValue(SAMPLE_IDS_FILE));

        mWriter = initialiseWriter(cmd.getOptionValue(OUTPUT_FILE));
    }

    public void run()
    {
        if(mSampleIds.isEmpty() || mWriter == null)
        {
            LOGGER.warning("initialisation failed");
            return;
        }

        for(int i = 0; i < mSampleIds.size(); ++i)
        {
            final String sampleId = mSampleIds.get(i);

            LOGGER.fine(String.format("%d: extracting data for sample(%s)", i, sampleId));
            extractSampleLocations(sampleId);

            if(i > 0 && (i % 100) == 0)
            {
                LOGGER.info("processed samples: " + i);
            }
        }

        try { mWriter.close(); } catch(IOException e) {}
    }

    private void loadSampleIds(final String sampleIdFile)
    {
        if (!Files.exists(Paths.get(sampleIdFile)))
            return;

        try
        {
            final List<String> lines = Files.readAllLines(new File(sampleIdFile).toPath());

            if(lines.isEmpty())
                return;

            if(lines.get(0).contains("SampleId"))
                lines.remove(0);

            mSampleIds.addAll(lines);

            LOGGER.info(String.format("loaded %d sampleIds from %s", mSampleIds.size(), sampleIdFile));
        }
        catch(IOException e)
        {
            LOGGER.severe("failed to load sampleIds file: " +e.toString());
        }
    }

    private BufferedWriter initialiseWriter(final String outputFileName)
    {
        try
        {
            BufferedWriter writer;

            Path outputPath = Paths.get(outputFileName);

            if (Files.exists(outputPath))
            {
                writer = Files.newBufferedWriter(outputPath, StandardOpenOption.TRUNCATE_EXISTING);
            }
            else
            {
                writer = Files.newBufferedWriter(outputPath, StandardOpenOption.CREATE);
            }

            writer.write(SampleLocationData.csvHeader());
            writer.newLine();

            return writer;
        }
        catch(IOException e)
        {
            LOGGER.severe("failed to create GCP sample data file: " + e.toString());
        }

        return null;
    }

    private void extractSampleLocations(final String sampleId)
    {
        try
        {
            final RemoteLocationsApi locations = new RemoteLocationsApi("hmf-crunch", sampleId);

            SampleLocationData sampleLocations = SampleLocationData.fromRemoteLocationsApi(sampleId, locations);

            mWriter.write(sampleLocations.csvData());
            mWriter.newLine();
        }
        catch(Exception e)
        {
            LOGGER.severe(String.format("failed to write to sample(%s) GCP locations data: %s", sampleId, e.toString()));
        }
    }

    public static void main(@NotNull final String[] args) throws ParseException
    {
        final Options options = new Options();

        options.addOption(SAMPLE_IDS_FILE, true, "Path to sample Ids file");
        options.addOption(OUTPUT_FILE, true, "Output file");

        final CommandLine cmd = createCommandLine(args, options);

        GcpSampleDataExtractor gcpSampleDataExtractor = new GcpSampleDataExtractor(cmd);
        gcpSampleDataExtractor.run();

        LOGGER.info("GCP sample data extraction complete");
    }

    @NotNull
    public static CommandLine createCommandLine(@NotNull final String[] args, @NotNull final Options options) throws ParseException
    {
        final CommandLineParser parser = new DefaultParser();
        return parser.parse(options, args);
    }

}
