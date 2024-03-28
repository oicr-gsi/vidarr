package ca.on.oicr.gsi.vidarr;

import java.time.Duration;
import java.util.Optional;

/**
 * The information required to run a local process
 *
 * @param standardInput data to be provided to standard input
 * @param maximumWait the maximum allowed runtime for a process
 * @param command the command to run and its arguments
 */
public record ProcessInput(
    Optional<byte[]> standardInput, Optional<Duration> maximumWait, String... command) {}
