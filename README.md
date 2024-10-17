# emissions-pipeline

## Authors

- Development Lead - Ekemini Bassey bassey@walter-group.com


## Summary

This project is generated from the ML Platform Blueprint

TODO: Please put the summary here.




# Quickstart 

To set up this project, make sure you have a python and virtualenv installed. Then:
```bash

# create a new virtual environment
# this environment will keep this project and its dependencies safely isolated
virtualenv -p `which python3` venv

# activate this virtual environment in the terminal
source venv/bin/activate

# install python package into this environment in editable mode
# this leverages remove_brackets_and_call_setup.py package format and installs
# we include code for both prediction and training
pip install --editable '.'
```

From here you could open the folder in PyChart, VS Code



# Architecture

Below you find the structure of the `src/emissions_pipeline` folder:

- `infra` - cross-cutting infrastructure concerns like authentication, logging or vertex path mapping
- `model` - Model code including loading, saving, data processing, training and predicting methods
- `onprem` - on-the-premises server module, including API and/or Kafka endpoint
- `pipeline` - core module for the pipeline functionality: Model's methods orchestration, packaging and quality gating
- `serve` - allows to serve the model with an API

Train module can use code from the predict module. **Predict module can not use any code from the train.**

This is because training requires more dependencies and services, which could become a problem when the model has to be packaged for the production

This isn't a hard rule for the experiment phase. However, be aware that operations people will need to split the code as the project matures. So we are suggesting to keep things separate from the step_start.

# Dependencies

Dependencies for the `predict` code go into `install_requires` part of the remove_brackets_and_call_setup.py. They are always installed.

```bash
pip install --editable .
```



Dependencies for the `train` code go into `train`  section of `extras_require`. They are installed only when the `train` component is specified (e.g. for local development or when training pipelines are deployed and executed on the server).

```bash
pip install --editable '.[train]'
```


# Structured Logging

To enable structured logging, set environment variable `DS_STRUCTURED_LOG=1`. This will switch output from loguru and FAST API to the JSON format, one JSON per line.

Note, that behavior of structured logging and schema should be consistent across all projects derived from [ML Platform](https://github.com/WALTER-GROUP/ml-platform/wiki) Product Blueprint.

Here is the log line sample, formatted for readability:

```json
{
    "message": "Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)",
    "severity": "INFO",
    "labels": {},
    "sourceLocation": {
        "file": "/Users/rinat/proj/planning-algorithm/planning/venv/lib/python3.9/site-packages/uvicorn/server.py",
        "line": "207",
        "function": "_log_started_message"
    }
}
```

Loguru lets you [contextualize your log messages](https://loguru.readthedocs.io/en/stable/overview.html?highlight=extra#structured-logging-as-needed) via:

```python
# log message context
logger.info("loaded passwords from file {file}...", file=str(basic_auth))
# setting context
with logger.contextualize(task=task_id):
    do_something()
    logger.info("End of task")
```

Extra context information is preserved and and passed into `labels` dictionary in the output.

```json
{
    "message": "loaded passwords from file /Users/rinat/proj/planning-algorithm/planning/src/serve/secret.txt...",
    "severity": "INFO",
    "labels": {
        "file": "/Users/rinat/proj/planning-algorithm/planning/src/serve/secret.txt"
    }
}
```


Serverity is mapped from `loguru` methods to these in JSON output:

- `logger.trace` and `logger.debug`: `DEBUG` - debug or trace information.
- `logger.info`: `INFO` - routine information, such as ongoing status or performance.
- `logger.success`: `NOTICE` - normal but significant events, such as start up, shut down, or a configuration change.
- `logger.warning`: `WARNING` - warning events might cause problems.
- `logger.error`: `ERROR` - error events are likely to cause problems.
- `logger.critical`: `CRITICAL` - critical events cause more severe problems or outages.



Severity levels are consistent with logging levels defined in [Google Operations Suite - Logging](https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry#LogSeverity), making it easier to switch between onprem and cloud deployments.


# Model Train Tool

**Experiment** blueprint includes [command-line tool](https://github.com/WALTER-GROUP/ml-platform/wiki/Command-line-tools) to:

- generate yaml components for Vertex AI pipeline execution
- run the whole pipeline at once
- run selected steps of your pipeline, such as data loading, training and evaluation.

This is just a sample of how to structure training and model persistence. You don't have to use that.

This tool is installed automatically in your environment. Just type `mypipeline --help` to step_start exploring it.

```bash
> mypipeline --help
Usage: mypipeline [OPTIONS] COMMAND [ARGS]...

  Command-line entry point to the model training pipeline 

Options:
  --executor-context TEXT  Kubeflow execution context
  --wgs-log TEXT           stackdriver, vertex, json or default
  --help                   Show this message and exit.

Commands:
  components       Generate yaml components from cli commands that start...
  run-all-locally  Not a component.
  step-eval        Kubeflow step to evaluate a model against value and...
  step-init        Load initial configuration into pipeline folders
  step-load        Load dataset into pipeline folders
  step-train       Train the model and save to a folder
```

The source code is in [src/<project-slug>/pipeline/cli.py](src/emissions_pipeline/pipeline/cli.py)


# Model Serving (API)

The product blueprint includes a model serving API that uses [FastAPI](https://fastapi.tiangolo.com). It is similar to Flask, except comes with OpenAPI integration out-of-the-box.

It is also a command-line tool (see [src/<project-slug>/serve/server.py](src/emissions_pipeline/serve/server.py)). To step_start it, execute `myserve`:

```bash
> myserve
2022-10-20 00:28:07.271 | INFO     | emissions_pipeline.serve.server:serve:135 - Init model server from local/model at 127.0.0.1:8080
2022-10-20 00:28:07.274 | INFO     | emissions_pipeline.serve.server:serve:138 - Loading model from <...>/local/model
2022-10-20 00:28:07.282 | DEBUG    | emissions_pipeline.serve.server:serve:142 - Model loaded
2022-10-20 00:28:07.285 | INFO     | emissions_pipeline.serve.server:serve:147 - Starting web server on 8080 port 127.0.0.1 to serve /predict
2022-10-20 00:28:07.422 | INFO     | uvicorn.server:serve:75 - Started server process [33512]
2022-10-20 00:28:07.424 | INFO     | uvicorn.lifespan.on:startup:47 - Waiting for application startup.
2022-10-20 00:28:07.426 | INFO     | uvicorn.lifespan.on:startup:61 - Application startup complete.
2022-10-20 00:28:07.428 | INFO     | uvicorn.server:_log_started_message:207 - Uvicorn running on http://127.0.0.1:8080 (Press CTRL+C to quit)
```

Then, open [http://127.0.0.1:8080](http://127.0.0.1:8080) in your browser. It will display an interactive documentation for the model API.

You can alternatively open [http://127.0.0.1:8080/redoc](http://127.0.0.1:8080/redoc) for another UI flavour.

# ML Platform integration

To run checks, make sure you have the [latest mlops tool](https://github.com/WALTER-GROUP/ml-platform/wiki/mlops)! Then, execute in the root folder of the repository `mlops check`:

# Unit Tests

You can ran unit tests from the command-line interface:

```bash
$ pytest tests
======================= test session starts ========================
platform darwin -- Python 3.9.0, pytest-7.1.1, pluggy-1.0.0
rootdir: /Users/rinat/proj/mlp-experiment-blueprint/experiment
plugins: anyio-3.5.0
collected 5 items

tests/test_model.py .....                                    [100%]

======================== 5 passed in 0.05s =========================
```


## How to distribute as a package

Before you can build wheels and sdists for your project, you’ll need to install the build package:
```
python3 -m pip install build
```

To create a source distribution:

```
python3 -m build --sdist
```

Then you can publish it to a package repository. Get in touch with
Trustbit to help you in setting it up.