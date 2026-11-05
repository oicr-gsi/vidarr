package ca.on.oicr.gsi.vidarr.oncoanalyser.submission;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record OncoAnalyserSubmission(
        String submissionId,
        String templateId,
        MolecularData molecularData) {
    public OncoAnalyserSubmission {
        Objects.requireNonNull(templateId);
        Objects.requireNonNull(molecularData);
    }
}
