package ca.on.oicr.gsi.vidarr.oncoanalyser.submission;

import java.util.Set;

public record SequencingData (String name, Set<Sample> sequencingData) {
}
