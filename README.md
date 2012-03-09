
Silk is a data model in-between relation and trees. The design of Silk data model aims to support parallel stream data processing in a cluster machine environment. Text format of Silk is provided for readability and cooperation with various types of programming languages. Binary format of Silk is used for efficient data processing. 

Silk Weaver is a data management system for handling massive amount of data sets. One of the major targets of Silk Weaver is bioinfomatics data; For example, personal genome analysis results, in which tera-bytes of data are commonly used. 



## Modules
* `silk-core`	Common utilities (e.g., logger, command-line parser, reflection utilities).
* `silk-model`	Defines the Silk data model
* `silk-text`	Parser/Writer of Silk text format
* `silk-weaver`	Parallel DBMS for Silk data
* `silk-workflow` Workflow manager

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

For developing silk, using IntelliJ with its scala plugin is recommended.
If you are Windows user, install cygwin (with git and make command).
Mintty is a good terminal for Cygwin shells.

### Good resources for learning Scala
* Programming in Scala (2nd Ed) http://www.artima.com/shop/programming_in_scala
* Scala documentation: http://docs.scala-lang.org/
  * Scala collection: http://docs.scala-lang.org/overviews/collections/introduction.html
* Scala School by Twitter inc. http://twitter.github.com/scala_school/

### Other resources
* SBT (simple build tool) https://github.com/harrah/xsbt/wiki
  * Silk uses sbt for building projects. See all project/SilkBuild.scala, which contains the build definitions.
* How to use Git: ProGit http://progit.org/
* ScalaTest http://www.scalatest.org/
   * UnitTest / Tests by specifications

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

## For repository maintainer

### Publishing artifact
* Local deploy

```
$ bin/sbt publish-local
```

* Remote deploy

```
$ bin/sbt publish
```
