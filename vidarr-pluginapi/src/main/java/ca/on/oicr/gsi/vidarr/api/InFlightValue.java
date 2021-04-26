package ca.on.oicr.gsi.vidarr.api;

public final class InFlightValue {

  private int currentInFlight;
  private int maxInFlight;

  public InFlightValue() {
    this.currentInFlight = 0;
    this.maxInFlight = 0;
  }

  public InFlightValue(int currentInFlight, int maxInFlight) {
    this.currentInFlight = currentInFlight;
    this.maxInFlight = maxInFlight;
  }

  public int getCurrentInFlight() {
    return currentInFlight;
  }

  public void setCurrentInFlight(int currentInFlight) {
    this.currentInFlight = currentInFlight;
  }

  public int getMaxInFlight() {
    return maxInFlight;
  }

  public void setMaxInFlight(int maxInFlight) {
    this.maxInFlight = maxInFlight;
  }
}
