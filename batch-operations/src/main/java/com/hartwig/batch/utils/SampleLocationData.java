package com.hartwig.batch.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.logging.Logger;

import com.google.common.collect.Maps;
import com.hartwig.batch.api.RemoteLocationsApi;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

public class SampleLocationData
{
    public final String SampleId;
    public final String ReferenceId;
    public final String RunBucket;
    public final String GermlineVcf;
    public final String SageVcf;
    public final String GridssVcf;
    public final String GripssVcf;
    public final String PurpleVcf;
    public final String Purple;
    public final String Amber;
    public final String Cobalt;

    private static final Logger LOGGER = Logger.getLogger(SampleLocationData.class.getName());

    public static final String PURPLE_GENE_COPY_NUMBER = "purple.cnv.gene.tsv";
    public static final String PURPLE_QC = "purple.qc";
    public static final String PURPLE_PURITY = "purple.purity.tsv";

    public static final String OLD_BUCKET_PREFIX = "hmf-output";

    public SampleLocationData(
            final String sampleId, final String referenceId, final String runBucket, final String germlineVcf, final String sageVcf,
            final String gridssVcf, final String gripssVcf, final String purpleVcf, final String purple, final String amber,
            final String cobalt)
    {
        SampleId = sampleId;
        ReferenceId = referenceId;
        RunBucket = runBucket;
        GermlineVcf = germlineVcf;
        SageVcf = sageVcf;
        GridssVcf = gridssVcf;
        GripssVcf = gripssVcf;
        PurpleVcf = purpleVcf;
        Purple = purple;
        Amber = amber;
        Cobalt = cobalt;
    }

    public static SampleLocationData fromRemoteLocationsApi(final String sampleId, final RemoteLocationsApi locations)
    {
        // look for a bucket matching the sampleId to use by default
        String runBucket = locations.getSomaticVariantsPurple().bucket();

        String purpleSomaticVcf = locations.getSomaticVariantsPurple().path();
        String purpleDir = purpleSomaticVcf.substring(0, purpleSomaticVcf.lastIndexOf("/"));

        /* BAMs currently excluded
        String tumorBamPath = locations.getTumorAlignment().path();
        String tumorDir = locations.getTumorAlignment().asDirectory().toString();
        String tumorBam = locations.getTumorAlignment().toString();
        String tumorBucket = locations.getTumorAlignment().bucket();
        */

        return new SampleLocationData(
                sampleId, locations.getReference(), runBucket,
                extractPath(sampleId, locations.getGermlineVariantsSage()),
                extractPath(sampleId, locations.getSomaticVariantsSage()),
                extractPath(sampleId, locations.getStructuralVariantsGridss()),
                extractPath(sampleId, locations.getStructuralVariantsGripss()),
                purpleSomaticVcf, purpleDir,
                extractPath(sampleId, locations.getAmber()), extractPath(sampleId, locations.getCobalt()));
    }

    private static String extractPath(final String sampleId, final GoogleStorageLocation location)
    {
        String bucket = location.bucket();

        if(bucket.toUpperCase().contains(sampleId))
            return location.path();
        else
            return location.bucket() + "/" + location.path();
    }

    private String getFileOnly(final String fileRef)
    {
        // strip version and directory data
        String[] fileData = fileRef.split("/");
        return fileData[fileData.length - 1];
    }

    public String localFileRef(final String fileRef)
    {
        String fileRefOnly = getFileOnly(fileRef);
        return String.format("%s/%s", VmDirectories.INPUT, fileRefOnly);
    }

    public String formDownloadRequest(final String fileRef)
    {
        String remotePath = fileRef.contains(OLD_BUCKET_PREFIX) ? fileRef : String.format("%s/%s", RunBucket, fileRef);
        return String.format("gsutil -m -u hmf-crunch cp -r gs://%s %s/", remotePath, VmDirectories.INPUT);
    }

    public String remotePurpleFile(final String fileSuffix)
    {
        return String.format("gs://%s/%s/%s.%s" + RunBucket, Purple, SampleId, fileSuffix);
    }

    private static final String DELIM = ",";

    public static String csvHeader()
    {
        StringJoiner sj = new StringJoiner(DELIM);
        sj.add("SampleId");
        sj.add("ReferenceId");
        sj.add("RunBucket");
        sj.add("GermlineVcf");
        sj.add("SageVcf");
        sj.add("GridssVcf");
        sj.add("GripssVcf");
        sj.add("PurpleVcf");
        sj.add("Purple");
        sj.add("Amber");
        sj.add("Cobalt");
        return sj.toString();
    }

    public String csvData()
    {
        StringJoiner sj = new StringJoiner(DELIM);
        sj.add(SampleId);
        sj.add(ReferenceId);
        sj.add(RunBucket);
        sj.add(GermlineVcf);
        sj.add(SageVcf);
        sj.add(GridssVcf);
        sj.add(GripssVcf);
        sj.add(PurpleVcf);
        sj.add(Purple);
        sj.add(Amber);
        sj.add(Cobalt);
        return sj.toString();
    }

    public static Map<String,SampleLocationData> loadSampleLocations(final String filename, final List<String> restrictedSampleIds)
    {
        Map<String,SampleLocationData> sampleLocations = Maps.newHashMap();

        try
        {
            BufferedReader fileReader = new BufferedReader(new FileReader(filename));
            String header = fileReader.readLine();

            final String[] columns = header.split(DELIM,-1);
            final Map<String,Integer> fieldsIndexMap = Maps.newLinkedHashMap();

            for(int i = 0; i < columns.length; ++i)
            {
                fieldsIndexMap.put(columns[i], i);
            }

            int sampleIndex = fieldsIndexMap.get("SampleId");
            int refIndex = fieldsIndexMap.get("ReferenceId");
            int bucketIndex = fieldsIndexMap.get("RunBucket");
            int germlineIndex = fieldsIndexMap.get("GermlineVcf");
            int sageIndex = fieldsIndexMap.get("SageVcf");
            int gridssIndex = fieldsIndexMap.get("GridssVcf");
            int gripssIndex = fieldsIndexMap.get("GripssVcf");
            int purpleVcfIndex = fieldsIndexMap.get("PurpleVcf");
            int purpleIndex = fieldsIndexMap.get("Purple");
            int amberIndex = fieldsIndexMap.get("Amber");
            int cobaltIndex = fieldsIndexMap.get("Cobalt");

            String line = "";

            while((line = fileReader.readLine()) != null)
            {
                final String[] values = line.split(DELIM, -1);

                String sampleId = values[sampleIndex];

                if(!restrictedSampleIds.isEmpty() && !restrictedSampleIds.contains(sampleId))
                    continue;

                sampleLocations.put(sampleId, new SampleLocationData(
                        sampleId, values[refIndex], values[bucketIndex], values[germlineIndex], values[sageIndex], values[gridssIndex],
                        values[gripssIndex], values[purpleVcfIndex], values[purpleIndex], values[amberIndex], values[cobaltIndex]));
            }

            LOGGER.info(String.format("loaded %s sample locations from file(%s)", sampleLocations.size(), filename));
        }
        catch(IOException e)
        {
            LOGGER.severe("failed to read sample locations file: {}" + e.toString());
        }

        return sampleLocations;
    }
}
