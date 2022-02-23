# Víðarr Prometheus Alertmanager plugin

Víðarr can be configured to use a Prometheus Alertmanager instance as a 
consumable resource, and inhibit workflow runs if alerts matching the Vidarr 
configuration are firing.

## AutoInhibit alert consumable resource
Here is an example of how to configure the 
[Víðarr config file](../admin-guide.md) to inhibit workflow runs when 
Alertmanager serves an alert with labels 
`alertname="AutoInhibit",environment="production",job="vidarr"`:

```json
{
  "consumableResources": {
    "autoinhibit-production": {
      "type": "alertmanager-auto-inhibit",
      "alertmanagerUrl": "http://an.alertmanager.url:9093",
      "autoInhibitOnEnvironment": "production",
      "cacheTtl": 1,
      "cacheRequestTimeout": 5,
      "labelsOfInterest": ["job"],
      "valuesOfInterest": ["vidarr"]
    }
  },
  ... and then further in the "targets" section ...
  "targets": {
    ...
    "consumableResources": ["autoinhibit-production"]
  }
}
```

### Configuration

` "type": "alertmanager-auto-inhibit"` sets this configuration for AutoInhibit
alerts only.

`"alertmanagerUrl"` sets the URL of the Alertmanager instance to query.

`"autoInhibitOnEnvironment"` should be set to the value of the alert's
`environment` label. If multiple environment values should be considered,
create a separate configuration for each environment.

`"cacheTtl"` is the duration, in minutes, that the alert cache is considered
fresh.

`"cacheRequestTimeout"` is the duration, in minutes, before the request to
Alertmanager times out.

`"labelsOfInterest"` is the set of labels whose values will be assessed for 
workflow run inhibition.

`"valuesOfInterest"` is the set of label values that will be assessed for 
workflow run inhibition. Internally, the _workflow name_ and _workflow name and version_* 
are added to this list of values of interest.

### Workflow run inhibition
In order for a match and a workflow run inhibition to occur, the alert must
have the following:
  * `alertname="AutoInhibit"`
  * `environment=<value specified by 'autoInhibitOnEnvironment' configuration value>`
  * one or more of the labels in `<labelsOfInterest>` whose value is:
    * one of the `<valuesOfInterest>`; or
    * _workflow name_; or
    * _workflow name and version_*

*(formatted like 
_bcl2fastq_3_2_0_, where any periods in the version are converted to underscores)

The workflow run inhibition will last as long as the AutoInhibit alert is 
firing.