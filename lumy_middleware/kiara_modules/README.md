# Kiara modules

This package contains Kiara modules and pipelines that have not been accepted into curated Kiara packages (or removed) but are still essential for Kiara workflows with custom types data values that need to be rendered as UI elements in Lumy.

## Modules groups

- `network_analysis` - contains modules that convert `network_graph` custom type into an Arrow table of `nodes` or `edges`.
- `network_analysis_temporary` - contains modules used in Network Analysis workflow that deal with selecting data repository table items used in the workflows and building network graph `nodes` and `edges` tables out of these items. These modules predate data repository implementation in Kiara and should be removed once Network Analysis workflow is updated.
- `table` - contains modules that have been removed from `kiara_modules.core` package without consulting with Lumy developers (see [commented out code here](https://github.com/DHARPA-Project/kiara_modules.core/blob/0781db54834ca6b3380941b27de0079603827e39/src/kiara_modules/core/import.py#L49)). This file should be removed once these modules are reintroduced in Kiara.

## Considerations

This is not the right place for these modules because they deal with data types that are not included into the default dependencies of the middleware. Data types used here are defined in Kiara modules that may be used as dependencies of certain workflows that will be installed separately by Lumy when such workflows are first used in the app (`network_graph` data type is an example: it is defined in `kiara_modules.network_analysis` package which is a dependency of the `network_analysis` workflow but not a dependency of the middleware).

Consider convincing Kiara maintainers to include these modules into respective packages. Alternatively extract them into separate packages and add them as dependencies to respective workflows.

## Pipelines

The `pipelines` directory contains all pipelines files.

- `createTableFromFile` - pipeline that has been removed from `kiara_modules.core` but is needed for temporary onboarding screen in Kiara. Should be removed once proper onboarding is implemented.
- `networkAnalaysisWorkflow` - default network analysis workflow. Should be removed when it is refactored and then either accepted into `kiara_modules.network_analysis` or moved into a separate package.
