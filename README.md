
Silk is a data format in-between relation and trees.

## Modules
* `silk-core`	Common utilities
* `silk-lens`	    Mapping support between Objects and Silk
* `silk-model`	Defines the Silk data model
* `silk-parser`	Silk format parser
* `silk-writer`	Silk format writer
* `silk-store`	Database storage implementation for Silk
* `silk-weaver`	Parallel query processor for Silk data

## Installation

```
$ git clone git://github.com/xerial/silk.git
$ cd silk
$ make install  // silk command will be installed in $HOME/local/bin
```

To change the installation location, set PREFIX:

```
$ make package
$ su
# PREFIX=/usr/local make install  // silk command will be installed in /usr/local/bin
```



## For developers

### Checking out the source code
```
$ git clone git://github.com/xerial/silk.git
$ cd silk
```

### Install Silk to your $HOME/local/bin
```
$ cd silk
$ make install
```
Edit your PATH environment variable to see `$HOME/local/bin`

### Install Silk to somwhere else 
```
$ make dist
$ sudo -u bio make install PREFIX=/bio
```
The silk command will be installed to `/bio/bin/silk` and folder `/bio/silk/(silk-version)` will be 
created to store library and configuration files.


### Compile 

```
$ bin/sbt compile
```

or

```
$ make compile
```

### Run tests with sbt

```
$ make test
```

### Create InteliJ IDEA project files

```
$ make idea
```

### Accelerating Your Development Cycle

Continuously build while editing source codes:

    $ bin/sbt ~test:compile

Run a specific test repeatedly:

    $ bin/sbt -Dloglevel=debug
    > ~test-only (test class name) 

You can use wild-card (`*`) to specify test class names:
```
> ~test-only *OptionParserTest
```


