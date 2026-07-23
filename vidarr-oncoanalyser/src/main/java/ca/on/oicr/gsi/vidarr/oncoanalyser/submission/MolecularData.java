package ca.on.oicr.gsi.vidarr.oncoanalyser.submission;

public record MolecularData (String sequencingPlatform, Sample tumorSample, Sample referenceSample,
                             Sample tumorRnaSample) {}
