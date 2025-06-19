# How to install this utility module

## Usage

For direct usage and integrating the utility module into other plugins or codes, `nomad-utility-workflows` is available as a PyPI package:

```bash
pip install nomad-utility-workflows>=0.2.0
```

If you are following the How-to guides in the docs, use the "vis" optional dependencies tag (includes dependencies for, e.g., Jupyter notebook and graph visualization):

```bash
pip install nomad-utility-workflows[vis]>=0.2.0
```

!!! Warning "Warning"
    There were breaking changes made in the transition to `nomad-utility-workflows` version 0.2.0. This is particularly relevant for the functions called for building the workflow yaml. To use older versions, you should reference the corresponding docs pages by checking out an older branch and serving the mkdocs page locally (See README.md).

!!! Warning "Warning"
    There were breaking changes made in the transition to `nomad-utility-workflows` version 0.1.0. This is particularly relevant for the structure of inputs of the workflow graph generation functions. To use older versions, you should reference the corresponding docs pages by checking out an older branch and serving the mkdocs page locally (See README.md).

### Linking to your NOMAD account
Create an account on https://nomad-lab.eu/.
Store your credentials in a `.env` file in your working directory, at the root plugin directory for developers, or in some directory that is added to your `PYTHONPATH`, with the following content
```bash
NOMAD_USERNAME="MyLogin"
NOMAD_PASSWORD="MyPassWord"
```
and insert your username and password.

!!! Warning "CAUTION FOR DEVELOPERS"
    Never push your `.env` file to a repository. This would expose your password.

### Running test Notebooks
To run the test notebooks, create a jupyter kernel using your venv:
```sh
python -m ipykernel install --user --name=nomad_utility_workflows
```

## Development

If you want to develop this module locally, clone the project and in the plugin folder, create a virtual environment (you can use Python 3.10, or 3.11):
```sh
git clone https://github.com/FAIRmat-NFDI/nomad-utility-workflows.git
cd nomad-utility-workflows
python3.11 -m venv .pyenv
. .pyenv/bin/activate
```

Make sure to have `pip` upgraded:
```sh
pip install --upgrade pip
```

We recommend installing `uv` for fast pip installation of the packages:
```sh
pip install uv
```

Install the `nomad-lab` package:
```sh
uv pip install '.[vis,dev]'
```

The plugin is still under development. If you would like to contribute, install the package in editable mode (with the added `-e` flag):
```sh
uv pip install -e '.[vis,dev]'
```

### Run the tests

You can run locally the tests:
```sh
python -m pytest -sv tests
```

where the `-s` and `-v` options toggle the output verbosity.

Our CI/CD pipeline produces a more comprehensive test report using the `pytest-cov` package. You can generate a local coverage report:
```sh
uv pip install pytest-cov
python -m pytest --cov=src tests
```

### Run linting and auto-formatting

We use [Ruff](https://docs.astral.sh/ruff/) for linting and formatting the code. Ruff auto-formatting is also a part of the GitHub workflow actions. You can run locally:
```sh
ruff check .
ruff format . --check
```


### Debugging

For interactive debugging of the tests, use `pytest` with the `--pdb` flag. We recommend using an IDE for debugging, e.g., _VSCode_. If that is the case, add the following snippet to your `.vscode/launch.json`:
```json
{
  "configurations": [
      {
        "name": "<descriptive tag>",
        "type": "debugpy",
        "request": "launch",
        "cwd": "${workspaceFolder}",
        "program": "${workspaceFolder}/.pyenv/bin/pytest",
        "justMyCode": true,
        "env": {
            "_PYTEST_RAISE": "1"
        },
        "args": [
            "-sv",
            "--pdb",
            "<path-to-plugin-tests>",
        ]
    }
  ]
}
```

where `<path-to-plugin-tests>` must be changed to the local path to the test module to be debugged.

The settings configuration file `.vscode/settings.json` automatically applies the linting and formatting upon saving the modified file.


### Documentation on Github pages

To view the documentation locally, install the related packages using:
```sh
uv pip install -r requirements_docs.txt
```

Run the documentation server:
```sh
mkdocs serve
```

