package ca.on.oicr.gsi.vidarr.oncoanalyser.submission;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record MolecularData (String sequencingPlatform, Sample tumorSample, Sample referenceSample,
                             Sample tumorRnaSample) {
    public MolecularData {
        Objects.requireNonNull(sequencingPlatform);
    }
}
