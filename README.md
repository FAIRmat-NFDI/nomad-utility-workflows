# nomad-utility-workflows plugin

Utilities for interfacing with NOMAD within workflows, e.g., via a workflow manager, including python API functions for uploading to NOMAD and querying the repository as well as automated generation of NOMAD custom workflow yaml file ([NOMAD workflow data models](https://github.com/nomad-coe/nomad-schema-plugin-simulation-workflow)).

This `nomad` plugin was generated with `Cookiecutter` along with `@nomad`'s [`cookiecutter-nomad-plugin`](https://github.com/FAIRmat-NFDI/cookiecutter-nomad-plugin) template.


## Development

If you want to develop locally this plugin, clone the project and in the plugin folder, create a virtual environment (you can use Python 3.9, 3.10, or 3.11):
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
uv pip install '.[dev]' --index-url https://gitlab.mpcdf.mpg.de/api/v4/projects/2187/packages/pypi/simple
```

**Note!**
Until we have an official pypi NOMAD release with the plugins functionality make
sure to include NOMAD's internal package registry (via `--index-url` in the above command).

The plugin is still under development. If you would like to contribute, install the package in editable mode (with the added `-e` flag):
```sh
uv pip install -e '.[dev]' --index-url https://gitlab.mpcdf.mpg.de/api/v4/projects/2187/packages/pypi/simple
```

### Linking to your NOMAD account
Create an account on https://nomad-lab.eu/.
Store your credentials in a `.env` file at the root plugin directory, with the following content
```bash
NOMAD_USERNAME="MyLogin"
NOMAD_PASSWORD="MyPassWord"
```
and insert your username and password.

> [!CAUTION]
> Never push your `.env` file to a repository. This would expose your password.

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

### Test Notebooks
To run the test notebooks, create a jupyter kernel using your venv:
```sh
python -m ipykernel install --user --name=nomad_utility_workflows
```


## Adding this plugin to NOMAD

Currently, NOMAD has two distinct flavors that are relevant depending on your role as an user:
1. [A NOMAD Oasis](#adding-this-plugin-in-your-nomad-oasis): any user with a NOMAD Oasis instance.
2. [Local NOMAD installation and the source code of NOMAD](#adding-this-plugin-in-your-local-nomad-installation-and-the-source-code-of-nomad): internal developers.

### Adding this plugin in your NOMAD Oasis

Read the [NOMAD plugin documentation](https://nomad-lab.eu/prod/v1/staging/docs/howto/oasis/plugins_install.html) for all details on how to deploy the plugin on your NOMAD instance.

### Adding this plugin in your local NOMAD installation and the source code of NOMAD

Modify the text file under `/nomad/default_plugins.txt` and add:
```sh
<other-content-in-default_plugins.txt>
nomad-utility-workflows==x.y.z
```
where `x.y.z` represents the released version of this plugin.

Then, go to your NOMAD folder, activate your NOMAD virtual environment and run:
```sh
deactivate
cd <route-to-NOMAD-folder>/nomad
source .pyenv/bin/activate
./scripts/setup_dev_env.sh
```

Alternatively and only valid for your local NOMAD installation, you can modify `nomad.yaml` to include this plugin, see [NOMAD Oasis - Install plugins](https://nomad-lab.eu/prod/v1/staging/docs/howto/oasis/plugins_install.html).


### Build the python package

The `pyproject.toml` file contains everything that is necessary to turn the project
into a pip installable python package. Run the python build tool to create a package distribution:

```sh
pip install build
python -m build --sdist
```

You can install the package with pip:

```sh
pip install dist/nomad-utility-workflows-0.1.0
```

Read more about python packages, `pyproject.toml`, and how to upload packages to PyPI
on the [PyPI documentation](https://packaging.python.org/en/latest/tutorials/packaging-projects/).


### Template update

We use cruft to update the project based on template changes. A `cruft-update.yml` is included in Github workflows to automatically check for updates and create pull requests to apply updates. Follow the [instructions](https://github.blog/changelog/2022-05-03-github-actions-prevent-github-actions-from-creating-and-approving-pull-requests/) on how to enable Github Actions to create pull requests.

To run the check for updates locally, follow the instructions on [`cruft` website](https://cruft.github.io/cruft/#updating-a-project).


## Main contributors
| Name | E-mail     |
|------|------------|
| Joseph F. Rudzinski | [joseph.rudzinski@physik.hu-berlin.de](mailto:joseph.rudzinski@physik.hu-berlin.de)
