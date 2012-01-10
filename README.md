
Silk is a data format in between relation and trees. 



## Run tests with sbt

    $ JAVA_OPT="-Dloglevel=debug" sbt test


## Create InteliJ IDEA project files
   
First build the latest version of sbt-idea plugin that set 'use fsc' option by default:

    $ git clone git://github.com/mpeltonen/sbt-idea.git 
    $ cd sbt-idea
    $ sbt publish-local


    $ sbt "gen-idea no-sbt-classifiers"


