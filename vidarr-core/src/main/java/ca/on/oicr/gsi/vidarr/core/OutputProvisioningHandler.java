package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.api.ExternalId;
import java.util.Map;
import java.util.Set;

/**
 * A consumer of provisioning output
 *
 * @param <TX> the type of a transaction for the provisioner
 */
public interface OutputProvisioningHandler<TX> {

  /**
   * Provision out a file
   *
   * @param ids the external IDs associated with this file
   * @param storagePath the permanent storage path of the file
   * @param md5 the MD5 hash of the file's contents
   * @param metatype the MIME type of the file
   * @param fileSize the size of the file, in bytes
   * @param labels additional data attributes associated with this file
   * @param transaction the transaction to update the information in
   */
  void provisionFile(
      Set<? extends ExternalId> ids,
      String storagePath,
      String md5,
      String metatype,
      long fileSize,
      Map<String, String> labels,
      TX transaction);

  /**
   * Provision out a URL
   *
   * @param ids the external IDs associated with this file
   * @param url the URL of the data recorded in an external system
   * @param labels additional data attributes associated with this file
   * @param transaction the transaction to update the information in
   */
  void provisionUrl(
      Set<? extends ExternalId> ids, String url, Map<String, String> labels, TX transaction);
}
