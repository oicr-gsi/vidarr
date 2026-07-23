package ca.on.oicr.gsi.vidarr.oncoanalyser.submission;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ //
        @JsonSubTypes.Type(value = Fastq.class, name = "FASTQ"), //
        @JsonSubTypes.Type(value = Bam.class, name = "BAM"), //
}) //
public class SequencingData {}
