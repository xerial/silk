# Silk: A framework for managing SQL data flows.

http://xerial.org/silk

## Examples

```scala
import xerial.silk.core._

import sampledb._

// SELECT count(*) FROM nasdaq
def dataCount = nasdaq.size

// SELECT time, close FROM nasdaq WHERE symbol = 'APPL'
def appleStock = nasdaq.filter(_.symbol is "APPL").select(_.time, _.close)

// You can use a raw SQL statjement as well:
def appleStockSQL = sql"SELECT time, close FROM nasdaq where symbol = 'APPL'"

// SELECT time, close FROM nasdaq WHERE symbol = 'APPL' LIMIT 10
appleStock.limit(10).print

// time-column based filtering
appleStock.between("2015-05-01", "2015-06-01")

for(company <- Seq("YHOO", "GOOG", "MSFT")) yield {
  nasdaq.filter(_.symbol is company).selectAll
}

```

## Milestones

 - Build SQL + local analysis workflows
 - Submit queries to Presto / Treasure Data
 - Run scheduled queries
 - Retry upon failures
 - Cache intermediate results
 - Resume workflow
 - Partial workflow executions
 - Sampling display
    - Interactive mode
 - Split a large query into small ones
    - Differential computation for time-series data

 - Windowing for stream queries

 - Object-oriented workflow

 - Input Source: fluentd/embulk
 - Output Source:

 - Workflow Executor
   - Local-only mode
   - Register SQL part to Treasure Data
   - Run complex analysis on local cache
   - UNIX command executor

