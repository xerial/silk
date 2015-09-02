---
layout: default
title: Silk*  Weaving SQL Workflows
tagline: 
---
{% include JB/setup %}

# Weaving Workflows with Silk

Silk is an open-source workflow description and execution framework, written in Scala. With Silk, you can create a workflow consisting of
 SQL, Unix commands, Scala, etc.

## Example

    def appleStock = nasdaq.filter(_.symbol is "APPL")
               .select(_.time, _.close)
               .orderBy(_.time)

    // show the latest 10 stock price information
    appleStock.limit(10).print

## Object-oriented workflow programming

A workflow in Silk is a class (or trait in Scala).
As in object-oriented programming style, you can hide the complex workflow within a class (encapsulation), override tasks in the
workflow and reuse existing workflows.


## Milestones

*  Build SQL + local analysis workflows
*  Submit queries to Presto / Treasure Data
*  Run scheduled queries
*  Retry upon failures
*  Cache intermediate results
*  Resume workflow
*  Partial workflow executions
*  Sampling display
   *  Interactive mode
*  Split a large query into small ones
   *  Differential computation for time-series data

*  Windowing for stream queries

*  Object-oriented workflow

*  Input Source: fluentd/embulk
*  Output Source:

*  Workflow Executor
  *  Local-only mode
  *  Register SQL part to Treasure Data
  *  Run complex analysis on local cache
  *  UNIX command executor
