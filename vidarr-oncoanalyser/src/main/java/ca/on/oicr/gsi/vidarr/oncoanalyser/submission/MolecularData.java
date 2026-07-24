package ca.on.oicr.gsi.vidarr.oncoanalyser.submission;

import java.util.Objects;

public record MolecularData (String sequencingPlatform, Sample tumorSample, Sample referenceSample,
                             Sample tumorRnaSample) {

    public MolecularData {
        Objects.requireNonNull(sequencingPlatform);
    }
}
