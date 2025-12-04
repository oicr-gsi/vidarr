package ca.on.oicr.gsi.vidarr.core;

/**
 * A consumer of reprovisioning output
 *
 * @param <TX> the type of a transaction for the provisioner
 */
public interface ReprovisioningHandler<TX> {

  /**
   * Reprovision a file
   * @param originalPath Where the file was originally
   * @param newPath Where the file has moved to
   * @param transaction The transaction to update the information in
   */
  void reprovisionFile(
      String originalPath,
      String newPath,
      TX transaction);
}
