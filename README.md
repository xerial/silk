
Silk is a data model in-between relation and trees. The design of Silk data model aims to support parallel stream data processing in a cluster machine environment. Text format of Silk is provided for readability and cooperation with various types of programming languages. Binary format of Silk is used for efficient data processing. 

Silk Weaver is a data management system for handling massive amount of data sets. One of the major targets of Silk Weaver is bioinfomatics data; For example, personal genome analysis results, in which tera-bytes of data are commonly used. 



## Modules
* `silk-core`
    * Silk data model definition
    * Parser/Writer of Silk text format
    * Lens for mapping between Scala objects and Silk data

    * Useful utilities
        * Logger trait
        * OptionParser - command line parser
        * SilkLauncher - invoke scala methods from command-line
        * ObjectSchema - Extract type information of scala classes
        * Reflection utilities

* `silk-weaver`	Parallel DBMS for Silk data

* `silk-workflow` Workflow manager

* `genome-lens` Data structures for genome sciences

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

### Publish Silk's jar files to local maven repository ($HOME/.m2/repository)
```
$ make local
```

## For developers

To develop silk, we recommend to use IntelliJ with its scala plugin.
If you are an Windows user, install cygwin (with git and make command).
Mintty is a good terminal for Cygwin shells.

### Good resources for learning Scala
* Programming in Scala (2nd Ed) http://www.artima.com/shop/programming_in_scala
* Scala documentation: http://docs.scala-lang.org/
  * Scala collection: http://docs.scala-lang.org/overviews/collections/introduction.html
  * The Architecture of Scala Collections: http://docs.scala-lang.org/overviews/core/architecture-of-scala-collections.html
* Scala School by Twitter inc. http://twitter.github.com/scala_school/

### Other resources
* SBT (simple build tool) https://github.com/harrah/xsbt/wiki
  * Silk uses sbt for building projects. See also project/SilkBuild.scala, which contains the build definitions.
* How to use Git: ProGit http://progit.org/
* Git Cheat Sheet: http://help.github.com/git-cheat-sheets/
* ScalaTest http://www.scalatest.org/
   * UnitTest / Tests by specifications

### Install Scala API locally
You can browse scala API (http://www.scala-lang.org/api/current/index.html) while you are offline. Get the latest version of scala, then use (scala_package)/bin/sbaz command to download the entire API document to your machine:

```
$ sbaz install scala-documentation
```

### Configure Your Git
```
# Set user name for git commit
$ git config --global user.name "(Your full name)"
$ git config --global user.email "xyz@xxxx.yyy"

## Enforce use of LF (never translate LF to CRLF, the default style in Windows)
$ git config --global core.eol lf
```

In IntelliJ, set Unix-like EOL style in ```Settings -> Code Style -> General -> Line separator (for new files)```.

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

### Accelerate your development cycle

Continuously build the project while editing source codes:

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

Show full-stack trace of ScalaTest:
```
> ~test-only *Test -- -oF
```

* http://www.scalatest.org/user_guide/using_scalatest_with_sbt

### Useful GNU screen setting

Add the following to $HOME/.screenrc:
```
termcapinfo xterm* ti@:te@
```
And also if your are using iTerm2, go to Preference -> Terminal, then turn on 'Save lines to scrollback when an app status bar is present'.

These settings enable buffer scrolling via mouse when you are using screen.

## For repository maintainer

### Publishing artifact
* Local deploy (to $HOME/.m2/repository)

```
$ bin/sbt publish-local
```

* Remote deploy

```
$ bin/sbt publish
```
