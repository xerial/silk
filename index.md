---
layout: default
title: Silk Weaver - A Novel Database for Relations, Trees and Streams
tagline: A new data model for relations, trees and streams
---
{% include JB/setup %}


## Scaling Your Data
**Silk** is a new data model for describing **relations** (tables), **trees** and **streams**. This flexible data model enables us to manage various types of data in a unified framework. 

**Silk Weaver** is an open-source DBMS based on Silk data model. Silk Weaver is designed to handle massive amount of data sets using multi-core CPUs in cluster machines. Once mapping to Silk is established, your data is ready to process in parallel and distributed environment.

Silk supports handy mapping of structured data (e.g., JSON, XML), flat data (e.g., CVS, tap-separated data) and object data written in [Scala](http://scala-lang.org). Mappings between class objects and Silk are automatically generated, and no need exists to define mappings by hand.

Large volumes of data can be mapped into Silk by using data streams. In **genome sciences** tera-bytes of data are commonly used, and various types of biological formats need to be managed in stream style. Silk Weaver intergrates various data types used in bioinformatics (e.g., BED, WIG, FASTA, SAM/BAM formats etc.) and provides a uniform query interface which can be accessed from command-line or [Scala API](.).


