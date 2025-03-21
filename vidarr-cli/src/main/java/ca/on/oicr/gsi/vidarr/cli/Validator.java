package ca.on.oicr.gsi.vidarr.cli;

import ca.on.oicr.gsi.vidarr.api.ExternalId;
import ca.on.oicr.gsi.vidarr.core.OutputProvisioningHandler;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface Validator extends OutputProvisioningHandler<Void> {
  static Validator all(Stream<Validator> validators) {
    return new Validator() {
      final List<Validator> delegates = validators.collect(Collectors.toList());

      @Override
      public boolean validate(String id) {
        return delegates.stream().allMatch(validator -> validator.validate(id));
      }

      @Override
      public void provisionFile(
          Set<? extends ExternalId> ids,
          String storagePath,
          String checksum,
          String checksumType,
          String metatype,
          long fileSize,
          Map<String, String> labels,
          Void transaction) {
        for (final var validator : delegates) {
          validator.provisionFile(ids, storagePath, checksum, checksumType, metatype, fileSize, labels, transaction);
        }
      }

      @Override
      public void provisionUrl(
          Set<? extends ExternalId> ids, String url, Map<String, String> labels, Void transaction) {
        for (final var validator : delegates) {
          validator.provisionUrl(ids, url, labels, transaction);
        }
      }
    };
  }

  boolean validate(String id);
}
