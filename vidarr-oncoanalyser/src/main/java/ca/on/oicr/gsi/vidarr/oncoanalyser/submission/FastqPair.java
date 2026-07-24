package ca.on.oicr.gsi.vidarr.oncoanalyser.submission;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record FastqPair(String lane, String library, String laneR1Path, String laneR2Path, ReadGroup readGroup) {
}
