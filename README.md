
Silk is a data model in-between relation and trees. The design of Silk data model aims to support parallel stream data processing in a cluster machine environment. Text format of Silk is provided for readability and cooperation with various types of programming languages. Binary format of Silk is used for efficient data processing. 

Silk Weaver is a data management system for handling massive amount of data sets. One of the major targets of Silk Weaver is bioinfomatics data; For example, personal genome analysis results, in which tera-bytes of data are commonly used. 



## Modules
* `silk-core`	Common utilities (e.g., logger, option parser, etc.)
* `silk-lens`	Mapping support between Objects and Silk
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
# Pre-compile the distribution package
$ make dist
$ su
# PREFIX=/usr/local make install  // silk command will be installed in /usr/local/bin
```



## For developers

### Check out the source code
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

### Acceleratie your development cycle

Continuously build while editing source codes:

```	
# make debug is a short cut of 'bin/sbt -Dloglevel=debug'
$ make debug    
> ~test:compile
```

Run a specific test repeatedly:

```
$ make debug
> ~test-only (test class name) 
```

You can use wild-card (`*`) to specify test class names:
```
> ~test-only *OptionParserTest
```


