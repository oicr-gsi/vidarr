package ca.on.oicr.gsi.vidarr.core;

public interface ReprovisioningHandler<TX> {

  /**
   * TODO write me
   * @param originalPath
   * @param newPath
   * @param transaction
   */
  void reprovisionFile(
      String originalPath,
      String newPath,
      TX transaction);
}
