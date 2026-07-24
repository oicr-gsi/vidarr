package ca.on.oicr.gsi.vidarr.oncoanalyser.submission;

import java.util.Objects;

public record Sample (String name, SequencingData sequencingData) {
    public Sample {
        Objects.requireNonNull(name);
        Objects.requireNonNull(sequencingData);
    }
}
