package ca.on.oicr.gsi.vidarr.oncoanalyser.submission;

public record MolecularData (SequencingData tumorSample, SequencingData referenceSample,
                             SequencingData tumorRnaSample) {}
