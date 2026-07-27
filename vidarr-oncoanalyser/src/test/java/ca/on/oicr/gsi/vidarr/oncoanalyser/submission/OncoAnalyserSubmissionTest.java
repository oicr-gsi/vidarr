package ca.on.oicr.gsi.vidarr.oncoanalyser.submission;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;

import java.nio.file.Path;
import java.util.Set;

public class OncoAnalyserSubmissionTest {
    public static final JsonMapper MAPPER =
            JsonMapper.builder()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .configure(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS, true)
                    .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
                    .configure(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION, true)
                    .build();

    public static OncoAnalyserSubmission submission;
    public static Path testFile = Path.of("./src/test/resources/submission.json");

    @BeforeClass
    public static void setUpObject(){
        submission = new OncoAnalyserSubmission(
            "TEST_CASE",
                "TEST_TEMPLATE",
                new MolecularData(
                        "IMAGINARY_SEQUENCER",
                        new Sample(
                                "TUMOUR_SAMPLE",
                                new Fastq(
                                        Set.of(
                                                new FastqPair("ONE", "TUMOUR_SAMPLE_LIBRARY", "TUMOUR_SAMPLE_R1.fastq.gz", "TUMOUR_SAMPLE_R2.fastq.gz",
                                                        new ReadGroup("TUMOUR_SAMPLE_ID", "TUMOUR_SAMPLE_BC", "TEST", "TUMOUR_SAMPLE_DS", "TUMOUR_SAMPLE_DT","TUMOUR_SAMPLE_FO", "TUMOUR_SAMPLE_KS", "TUMOUR_SAMPLE_LB", "TUMOUR_SAMPLE_PG", 365, "TUMOUR_SAMPLE_PL", "TUMOUR_SAMPLE_PM", "TUMOUR_SAMPLE_PU", "TUMOUR_SAMPLE_SM")
                                        )
                                ))),
                        new Sample("REFERENCE_SAMPLE",
                                new Fastq(
                                        Set.of(
                                                new FastqPair(
                                                        null, null, "REFERENCE_SAMPLE_R1.fastq.gz", "REFERENCE_SAMPLE_R2.fastq.gz", new ReadGroup("REFERENCE_SAMPLE_ID", null, "TEST", null, null, null, null, "REFERENCE_SAMPLE_LB", null, null, null, null, "REFERENCE_SAMPLE_PU", "REFERENCE_SAMPLE_SM")
                                                ), new FastqPair(
                                                        null, null, "REFERENCE_SAMPLE_2_R1.fastq.gz", "REFERENCE_SAMPLE_2_R2.fastq.gz", new ReadGroup("REFERENCE_SAMPLE_2_ID", null, "TEST", null, null, null, null, "REFERENCE_SAMPLE_2_LB", null, null, null, null, "REFERENCE_SAMPLE_2_PU", "REFERENCE_SAMPLE_2_SM")
                                                    )
                                                )
                                )),
                        new Sample("TUMOUR_RNA_SAMPLE",
                                new Bam("TUMOUR_RNA_SAMPLE.bam", "TUMOUR_RNA_SAMPLE.bai"))));
    }


    @Test
    public void deserialize(){
        MAPPER.readValue(testFile, OncoAnalyserSubmission.class);
    }

    @Test
    public void serialize(){
        MAPPER.writeValueAsString(submission);
    }

    @Test
    public void equal() {
        // Deserialized object is the same as the json
        Assert.assertEquals(submission, MAPPER.readValue(testFile, OncoAnalyserSubmission.class));
    }
}
