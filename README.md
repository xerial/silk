
Silk is a data format in-between relation and trees.

## Modules
* silk-core	Common utilities
* silk-lens	Mapping support between Objects and Silk
* silk-model	Defines the Silk data model
* silk-parser	Silk format parser
* silk-writer	Silk format writer
* silk-store	Database storage implementation for Silk
* silk-weaver	Parallel query processor for Silk data


## For developers
### Run tests with sbt

    $ JAVA_OPT="-Dloglevel=debug" sbt test


### Create InteliJ IDEA project files

    $ sbt "gen-idea no-sbt-classifiers"


### Improving Development Cycle

Continuously build while editing source codes:

    $ sbt ~test:compile

Run a specific test repeatedly:

    $ sbt "~test-only (test class name)" -Dloglevel=debug

