package ca.on.oicr.gsi.vidarr.oncoanalyser.submission;

public record FastqPair(String lane, String library, String laneR1Path, String laneR2Path, ReadGroup readGroup) {
}
