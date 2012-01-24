
Silk is a data format between relation and trees.

## Modules
* silk-core



## Run tests with sbt

    $ JAVA_OPT="-Dloglevel=debug" sbt test


## Create InteliJ IDEA project files

    $ sbt "gen-idea no-sbt-classifiers"


## Development

Continuously build while editing source codes:
    $ sbt ~test:compile

Run a specific test repeatedly:
    $ sbt "~test-only (test class name)" -Dloglevel=debug

