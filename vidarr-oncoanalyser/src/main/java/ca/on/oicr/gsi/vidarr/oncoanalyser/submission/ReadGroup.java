package ca.on.oicr.gsi.vidarr.oncoanalyser.submission;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Objects;

/**
 * OncoAnalyser does not require any specific combination of ReadGroup fields; all combinations, including
 * none set, are valid. It is an OICR requirement that ID, SM, LB, PU, and CN be set.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ReadGroup(String ID, String BC, String CN, String DS, String DT, String FO, String KS, String LB,
                        String PG, Integer PI, String PL, String PM, String PU, String SM) {

    public ReadGroup {
        Objects.requireNonNull(ID);
        Objects.requireNonNull(SM);
        Objects.requireNonNull(LB);
        Objects.requireNonNull(PU);
        Objects.requireNonNull(CN);
    }
}
