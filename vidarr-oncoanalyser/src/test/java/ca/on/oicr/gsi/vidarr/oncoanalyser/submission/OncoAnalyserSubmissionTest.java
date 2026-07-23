package ca.on.oicr.gsi.vidarr.oncoanalyser.submission;

import org.junit.Test;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;

import java.nio.file.Path;

public class OncoAnalyserSubmissionTest {
    public static final JsonMapper MAPPER =
            JsonMapper.builder()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .configure(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS, true)
                    .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
                    .configure(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION, true)
                    .build();

    @Test
    public void parses(){
        MAPPER.readValue(Path.of("./src/test/resources/submission.json"), OncoAnalyserSubmission.class);
    }
}
