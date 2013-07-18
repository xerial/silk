## Silk: Fast cluster computing for data scientists.

Silk is an open-source cluster computing platform for data scientists.

### Features 

#### Distriuted data set
A data analysis program in Silk is based on a distributed data set `Silk[A]`, 
which will be distributed over the cluster machines. 
Silk provides distributed operations over `Silk[A]`, such as 
`map`, `filter`, `reduce`, `join`, `sort` etc.
These operations evaluated in the cluster and no need to write 
explicit parallelization or distributed code. 

#### Workflow managment

Makefile has been used for organizing complex data analysis workflows. 
It describes dependencies between tasks through files. 
This limits the available parallelism to the number of files created, 
and cannot be used to organize fine grained distributed schedules.

 * Workflows in Silk consists of a set of function calls, and dependencies between 
tasks are represented with dependencies between function calls. 
 * In Silk, users can choose an appropriate data transfer method between function calls; 
in-memory transfer, sending serailized objects through network, data files, etc.
 * When evaluating a function, Silk automatically detects dependencies (or input data) used in the function,
and creates a distributed schedule for evaluating these functions in a correct order.
 * Silk memorizes already computed data, and enables you to extend workflows 
without recomputing the whole data analysis. 

### Documentation
For the details of Silk, visit http://xerial.org/silk

## Contributors
 * Taro L. Saito (project architect)
 * Hayato Sakata
 * Jun Yoshimura
