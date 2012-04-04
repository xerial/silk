---
layout: default
title: Silk Weaver - A Novel Database for Relations, Trees and Streams
tagline: A new data model for relations, trees and streams
---
{% include JB/setup %}


### Silk: A Universal Data Model 
**Silk** is a new data model for describing **relations** (tables), **trees** and their **streams**. This flexible data model has the power to manage various types of data in a unified framework. 

* [Silk Data Model](model.html)

**Silk Weaver** is an open-source DBMS for Silk data, written in Scala. Silk Weaver is designed to process massive amount of data sets using multi-core CPUs in cluster machines. Once mapping to Silk is established, your data becomes ready for parallel and distributed computing.

* [Silk Weaver Overview](weaver.html)

To use various types of data at ease, Silk supports handy mapping of structured data (e.g., JSON, XML), flat data (e.g., CVS, tap-separated data) and object data written in [Scala](http://scala-lang.org). Mappings between class objects and Silk, called **Lens**, are automatically generated and no need exists to define lens by hand.

* [Lens: Mapping between Objects and Silk](lens.html)

### Silk Formats

Silk has a text format to increase the interoperability between programming languages. A machine-readable binary format is also provided to efficiently transfer data bewteen memory and disks. It is also possible to send Silk data between servers, which can be used to call remote functions (as known as RPC). Silk weaver can invoke remote functions even if they read data from streams. 

* [Silk Text Format](text-format.html)
* [Silk Binary Format](binary-format.html)

* [Silk RPC](rpc.html)

### Applications
Silk Weaver has a wide-range of applications; you can easily store your object data as Silk, and also data streams obtained through iterator interfaces can be mapped to Silk. In **genome sciences** tera-bytes of data are commonly used, and various types of biological formats need to be managed in stream style. Silk Weaver can integrate the data formats used in bioinformatics (e.g., BED, WIG, FASTA, SAM/BAM formats etc.) and provides a uniform query interface accessible from command-line or [Scala API](.).

### Silk Core Library
**silk-core** is a common library used in Silk Weaver. If you write programs in Scala, silk-core library would be useful outside the context of Silk Weaver. For example, **silk-core** contains: 

* Command-line option parser
* Logger 
* Performance measure of code blocks
* Object schema reader (parameters and methods defined in classes)
* Dynamic object construction library
* Remote function call
* Network data transfer
* Storing your object data in Silk format
* Process launcher (including JVM)
* etc.



