package ca.on.oicr.gsi.vidarr.oncoanalyser.submission;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record FastqPair(String lane, String library, String laneR1Path, String laneR2Path, ReadGroup readGroup) {
    public FastqPair {
        Objects.requireNonNull(laneR1Path);
        Objects.requireNonNull(laneR2Path);
    }
}
