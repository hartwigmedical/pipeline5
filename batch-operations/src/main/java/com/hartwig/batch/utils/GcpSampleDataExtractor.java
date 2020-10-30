package com.hartwig.batch.utils;

import static com.hartwig.batch.utils.PipelineOutputType.ALIGNED_READS;
import static com.hartwig.batch.utils.PipelineOutputType.GERMLINE_VCF;
import static com.hartwig.batch.utils.PipelineOutputType.SOMATIC_VARIANTS_PURPLE;
import static com.hartwig.batch.utils.PipelineOutputType.getApiFileKey;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.jetbrains.annotations.NotNull;


public class GcpSampleDataExtractor
{
    private static final Logger LOGGER = Logger.getLogger(GcpSampleDataExtractor.class.getName());

    private final String mApiCrtFile;
    private final String mApiKeyFile;
    private final String mOutputDir;
    private final List<String> mSampleIds;

    private BufferedWriter mWriter;

    public static final String SAMPLE_IDS_FILE = "sample_ids_file";
    public static final String OUTPUT_DIR = "output_dir";
    public static final String GCP_API_KEY_FILE = "gcp_api_key_file";
    public static final String GCP_API_CRT_FILE = "gcp_api_crt_file";

    public GcpSampleDataExtractor(final CommandLine cmd)
    {
        mSampleIds = Lists.newArrayList();
        loadSampleIds(cmd.getOptionValue(SAMPLE_IDS_FILE));

        mApiKeyFile = cmd.getOptionValue(GCP_API_KEY_FILE);
        mApiCrtFile = cmd.getOptionValue(GCP_API_CRT_FILE);

        mOutputDir = cmd.getOptionValue(OUTPUT_DIR);
        mWriter = initialiseWriter();
    }

    public void run()
    {
        if(mOutputDir == null || mSampleIds.isEmpty() || mWriter == null)
        {
            LOGGER.warning("initialisation failed");
            return;
        }

        int nextLog = 100;
        for(int i = 0; i < mSampleIds.size(); ++i)
        {
            final String sampleId = mSampleIds.get(i);

            if(!extractSampleData(sampleId))
            {
                writeSampleData(sampleId, false, "", "", "", "");
            }

            if(i >= nextLog)
            {
                LOGGER.info("processed samples: " + i);
                nextLog += 100;
            }
        }

        try { mWriter.close(); } catch(IOException e) {}
    }

    private boolean extractSampleData(final String sampleId)
    {
        final String gcpApiCall = String.format("curl --cert %s --key %s https://api.hartwigmedicalfoundation.nl/hmf/v1/datasets?biopsy=%s",
                mApiCrtFile, mApiKeyFile, sampleId);

        // curl -X GET https://postman-echo.com/get?foo1=bar1&foo2=bar2
        ProcessBuilder processBuilder = new ProcessBuilder(gcpApiCall.split(" "));

        try
        {
            processBuilder.directory(new File(mOutputDir));
            Process process = processBuilder.start();

            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = null;
            while ( (line = reader.readLine()) != null)
            {
                LOGGER.fine(String.format("sample(%s): %s", sampleId, line));

                JsonParser jsonParser = new JsonParser();
                final JsonElement mainElement = jsonParser.parse(line);
                final JsonObject mainObject = mainElement.getAsJsonObject();

                if(mainObject.entrySet().isEmpty())
                    return false;

                final String setId = mainObject.entrySet().iterator().next().getKey();

                final String germlineVcf = extractJsonPath(sampleId, mainObject, GERMLINE_VCF);

                if(germlineVcf == null)
                    return false;

                final String tumorCram = extractJsonPath(sampleId, mainObject, ALIGNED_READS);

                if(tumorCram == null)
                    return false;

                final String purpleVcf = extractJsonPath(sampleId, mainObject, SOMATIC_VARIANTS_PURPLE);

                if(purpleVcf == null)
                    return false;

                final String purpleDirectory = extractPurpleDirectory(purpleVcf);

                writeSampleData(sampleId, true, setId, tumorCram, germlineVcf, purpleDirectory);
            }
        }
        catch (Exception e)
        {
            LOGGER.severe("CURL API error: " + e.toString());
            return false;
        }

        return true;
    }

    private String extractPurpleDirectory(final String purpleVcf)
    {
        // gs://wide01010852t-dna-analysis/5.15/purple/WIDE01010852T.purple.somatic.vcf.gz
        final String[] items = purpleVcf.split("/");
        String purpleDirPath = "";
        for(int i = 0; i < items.length - 1; ++i)
        {
            purpleDirPath += items[i] + "/";
        }

        return purpleDirPath;
    }

    private String extractJsonPath(final String sampleId, final JsonObject mainObject, final PipelineOutputType fileType)
    {
        try
        {
            for(Map.Entry<String,JsonElement> entry : mainObject.entrySet())
            {
                final JsonElement fileElement = entry.getValue().getAsJsonObject().get(getApiFileKey(fileType));

                if(fileElement == null)
                {
                    LOGGER.warning(String.format("sample(%s) failed to find fileType(%s) ", sampleId, fileType));
                    return null;
                }

                final String sampleKey = fileElement.getAsJsonObject().has(sampleId) ?
                        sampleId : fileElement.getAsJsonObject().entrySet().iterator().next().getKey();
                final JsonObject fileObject = fileElement.getAsJsonObject().getAsJsonObject().getAsJsonObject(sampleKey);
                return fileObject.get("path").getAsString();
            }
        }
        catch (Exception e)
        {
            LOGGER.warning(String.format("sample(%s) failed to extract fileType(%s) ", sampleId, fileType));
        }

        return null;
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

    private BufferedWriter initialiseWriter()
    {
        String outputFileName = mOutputDir + "gcp_sample_data.csv";

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

            writer.write("SampleId,Valid,SetId,CramFile,GermlineVcf,PurpleDir");
            writer.newLine();
            return writer;
        }
        catch(IOException e)
        {
            LOGGER.severe("failed to create GCP sample data file: " + e.toString());
        }

        return null;
    }

    private void writeSampleData(final String sampleId, boolean valid, final String setId,
            final String cramFile, final String germlineVcf, final String purpleDir)
    {
        try
        {
            mWriter.write(String.format("%s,%s,%s,%s,%s,%s", sampleId, valid, setId, cramFile, germlineVcf, purpleDir));
            mWriter.newLine();
        }
        catch(IOException e)
        {
            LOGGER.severe("failed to write to line ref-genome bases: " + e.toString());
        }
    }

    public static void main(@NotNull final String[] args) throws ParseException
    {
        final Options options = new Options();

        options.addOption(SAMPLE_IDS_FILE, true, "Path to sample Ids file");
        options.addOption(GCP_API_KEY_FILE, true, "Path to API key file");
        options.addOption(GCP_API_CRT_FILE, true, "Path to API CRT file");
        options.addOption(OUTPUT_DIR, true, "Output data path");
        // options.addOption(LOG_DEBUG, false, "Log verbose");

        final CommandLine cmd = createCommandLine(args, options);

        // if(cmd.hasOption(LOG_DEBUG))
        //    Configurator.setRootLevel(Level.DEBUG);

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
