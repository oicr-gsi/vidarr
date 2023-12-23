package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.OutputProvisioner;
import ca.on.oicr.gsi.vidarr.OutputProvisioner.Result;
import ca.on.oicr.gsi.vidarr.RuntimeProvisioner;
import ca.on.oicr.gsi.vidarr.core.WrappedMonitor.RecoveryStarter;

public enum RecoveryType {
  RECOVER {
    @Override
    RecoveryStarter<Result> provisionOut(OutputProvisioner provisioner) {
      return provisioner::recover;
    }

    @Override
    RecoveryStarter<Result> runtime(RuntimeProvisioner provisioner) {
      return provisioner::recover;
    }
  },
  RETRY {
    @Override
    RecoveryStarter<Result> provisionOut(OutputProvisioner provisioner) {
      return provisioner::retry;
    }

    @Override
    RecoveryStarter<Result> runtime(RuntimeProvisioner provisioner) {
      return provisioner::retry;
    }
  };

  abstract RecoveryStarter<Result> provisionOut(OutputProvisioner outputProvisioner);

  abstract RecoveryStarter<Result> runtime(RuntimeProvisioner provisioner);
}
