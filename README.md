## Silk: Fast cluster computing for data scientists.

Silk is an open-source cluster computing platform for data scientists, written in Scala.

### Features 

#### Distriuted data set
A data analysis program in Silk uses distributed data set `Silk[A]`, 
which will be distributed over the cluster. Silk provides distributed operations over `Silk[A]`, including
`map`, `filter`, `reduce`, `join`, `sort` etc.

These operations are automatically distributed to the cluster, and no need exists to write 
explicit parallelization or distributed code. 

#### Workflow managment

Makefile has been used for organizing complex data analysis workflows,
which describes dependencies between tasks through input/output files. This limits the available parallelism to the number of files created in the workflow, 
and cannot be used to organize fine grained distributed schedules. Silk aims to be a replacement of Makefile.

 * Workflows in Silk consists of a set of function calls, and dependencies between 
tasks are represented with dependencies between function calls. 
 * In Silk users can choose an appropriate data transfer method between function calls; 
in-memory transfer, sending serailized objects through network, data files, etc.
 * When evaluating a function, Silk detects dependencies between functions,
then creates a distributed schedule for evaluating these functions in a correct order.
 * Silk memorizes already computed data, and enables you to extend workflows 
without recomputation.
 * Silk can call UNIX commands (as in Hadoop Streaming).

#### Object-oriented workflow programming

A workflow in Silk is a class (or trait in Scala). Which allows overriding 
existing workflows and combining several workflows to oragnize more complex one.


### Documentation
For the details of Silk, visit http://xerial.org/silk

## Contributors
 * Taro L. Saito (project architect)
 * Hayato Sakata
 * Jun Yoshimura
