## Testing a Víðarr Workflow Locally

To test a Víðarr workflow locally, ensure that you satisfy these requirements:

- Make sure the .sh file created from the instructions provided in the [Creating a Development Environment](https://github.com/oicr-gsi/vidarr/blob/master/admin-guide.md#creating-a-development-environment) step points to the jars you created in the [Installing Víðarr on a Linux Instance](https://github.com/oicr-gsi/vidarr/blob/master/admin-guide.md#installing-v%C3%AD%C3%B0arr-on-a-linux-instance) step.
- Use the JSON config file generated during that step in the configuration argument of your command.
    - Update the `"url"` block to a URL representing your environment, and the `"cromwellUrl"` block to your cromwell server's REST API.
- Follow the steps outlined in [Testing Workflows](https://github.com/oicr-gsi/vidarr/blob/master/admin-guide.md#testing-workflows) to compose a regression test file. This file will be used in the test argument of your command.
  - Another option is to use the [JSON test file](https://github.com/oicr-gsi/empty/blob/master/vidarrtest-regression.json.in) from the [empty](https://github.com/oicr-gsi/empty) repository.
      - Change the `"metrics_calculate"`, `"metrics_compare"`, and `"output_metrics"` blocks to your local files cloned from the [empty](https://github.com/oicr-gsi/empty) repository.
      - The `"configuration"` block can point to any local empty text file.
- Follow the steps from the [vidarr-tools README.md](https://github.com/oicr-gsi/vidarr-tools)  to create a local v.out file that will be used in the workflow argument of your command.

Finally, you have all the necessary files to test a Víðarr workflow locally, and you can run it using this command:
`./path/to/.sh_file test --configuration=path/to/config_file --test=path/to/test_file --workflow=path/to/workflow_file`