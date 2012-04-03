---
layout: default
title: Development Notes
---
{% include JB/setup %}
 
## Development Notes

### Installation

<pre class="prettyprint lang-sh">
$ git clone git://github.com/xerial/silk.git
$ cd silk
# Publish Silk's jar files to local maven repository ($HOME/.m2/repository)
$ make local
</pre>

To install command-line silk program:
<pre class="prettyprint">
$ make install  // silk command will be installed in $HOME/local/bin
</pre>

To change the installation location, set PREFIX:

<pre class="prettyprint lang-sh">
# Pre-compile the distribution package
$ make dist
$ sudo make PREFIX=/usr/local install  // silk command will be installed in /usr/local/bin
</pre>

#### Configure Your Git
<pre class="prettyprint">
# Set user name for git commit
$ git config --global user.name "(Your full name)"
$ git config --global user.email "xyz@xxxx.yyy"
# Force use of LF (never translate LF to CRLF, the default style in Windows)
$ git config --global core.eol lf
</pre>

In IntelliJ, set Unix-like EOL style in ```Settings -> Code Style -> General -> Line separator (for new files)```.

Without those settings, commit diff will be looked strange (as if every line is changed)

#### Check out the source code
<pre class="prettyprint">
$ git clone git://github.com/xerial/silk.git
$ cd silk
</pre>

#### Install Silk to your $HOME/local/bin
<pre class="prettyprint">
$ cd silk
$ make install
</pre>
Edit your PATH environment variable to see `$HOME/local/bin`


#### Compile 

<pre class="prettyprint">
$ bin/sbt compile
</pre>

or

<pre class="prettyprint">
$ make compile
</pre>


#### Run tests with sbt

<pre class="prettyprint">
$ make test
</pre>


#### Create InteliJ IDEA project files

<pre class="prettyprint">
$ make idea
</pre>

#### Accelerate your development cycle

Continuously build the project while editing source codes:

<pre class="prettyprint">
# make debug is a short cut of 'bin/sbt -Dloglevel=debug'
$ make debug    
> ~test:compile
</pre>


Run a specific test repeatedly:

<pre class="prettyprint">
$ make debug
> ~test-only (test class name) 
</pre>


You can use wild-card (`*`) to specify test class names:
<pre class="prettyprint">
> ~test-only *OptionParserTest
</pre>

Show full-stack trace of ScalaTest:
<pre class="prettyprint">
> ~test-only *Test -- -oF
</pre>


* http://www.scalatest.org/user_guide/using_scalatest_with_sbt

#### Useful GNU screen setting

Add the following to $HOME/.screenrc:
<pre class="prettyprint">
termcapinfo xterm* ti@:te@
</pre>

And also if your are using iTerm2, go to Preference -> Terminal, then turn on 'Save lines to scrollback when an app status bar is present'.

These settings enable buffer scrolling via mouse when you are using screen.

### For repository maintainer

#### Publishing artifact
* Local deploy (to $HOME/.m2/repository)

<pre class="prettyprint">
$ bin/sbt publish-local
</pre>

* Remote deploy

<pre class="prettyprint">
$ bin/sbt publish
</pre>
