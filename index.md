---
layout: default
title: Silk - Smart cluster computing for data scientists
tagline: 
---
{% include JB/setup %}

<div class="alert alert-info">
<strong>Warning</strong> This site is still in a preliminary state. Some pages linked from here may be missing and their writing is now in progress.
</div>


## Silk: Streaming Cluster Computing

Silk is an open-source cluster computing platform for data scientists, written in Scala.

### Features 

Silk has the following features for accelerating scientific data analysis:

#### Distriuted data set
A data analysis program in Silk uses distributed data set `Silk[A]`, 
which will be distributed over the cluster. Silk provides distributed operations over `Silk[A]`, including
`map`, `filter`, `reduce`, `join`, `sort` etc.

These operations are automatically distributed to the cluster, and no need exists to write 
explicit parallelization or distributed code. 

#### Workflow managment

Makefile has been used for organizing complex data analysis workflows,
which describes dependencies between tasks through input/output files. This limits the available parallelism to the number of files created in the workflow, so Makefile cannot be used to organize fine grained distributed schedules. Silk aims to be a replacement of Makefile.

* A task in Silk is a function call (or variable definition), and a workflow is a set of function calls.
 * Silk detects dependencies between function calls using Scala macros and JVM byte code analysis. 
* In Silk users can choose an appropriate data transfer method between function calls; 
in-memory transfer, sending serailized objects through network, data files, etc.
* When evaluating a function, Silk detects dependencies between functions,
then creates a distributed schedule for evaluating these functions in a correct order.
* Silk memorizes already computed data, and enables you to extend workflows 
without recomputation.
* Tracability. Silk records functions applied to `Silk[A]` data set. So you can trace how the resulting data
 are generated. 
* Silk can call UNIX commands (as in Hadoop Streaming).

#### Workflow queries
 * Intermediated data generated in the workflow can be queried, using a simple query syntax (relational-style query)
 * You can replace a part of the workflow data and execute partial workflows. This feature is useful for debugging data analysis programs, e.g. by using a small input data set.

#### Object-oriented workflow programming

A workflow in Silk is a just class (or trait in Scala) containing functions that use 
`Silk[A]` data set. This encupsulation of workflows allows overriding 
existing workflows, and also combining several workflows to oragnize more complex one.
This workflow programming style greatly helps reusing and sharing workflows.

### Contributors
 * Taro L. Saito (project architect)
 * Hayato Sakata
 * Jun Yoshimura
