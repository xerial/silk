//--------------------------------------
//
// LoadFileTest.scala
// Since: 2013/09/04 10:53 AM
//
//--------------------------------------

package xerial.silk.example

import xerial.silk.cluster.{Cluster3UserSpec, Cluster3Spec}
import java.io.{PrintWriter, BufferedWriter, FileWriter, File}
import xerial.silk.weaver.example.{ExampleMain, Person}

/**
 * @author Taro L. Saito
 */
class LoadFileTestMultiJvm1 extends Cluster3Spec {
  "parse file" in {
    start { env =>

    }
  }
}


class LoadFileTestMultiJvm2 extends Cluster3Spec {
  "parse file" in {
    start { env =>

    }
  }
}

class LoadFileTestMultiJvm3 extends Cluster3UserSpec {
  "parse file" in {

    // Create a temporary file
    info("preparing sample file")
    val file = File.createTempFile("load-test", ".tab", new File("target"))
    file.deleteOnExit()
    val out = new PrintWriter(new BufferedWriter(new FileWriter(file)))
    val N = 100000
    for(i <- (0 until N)) {
      val p = new Person(i, Person.randomName)
      out.println(p.toTSV)
    }
    out.close()
    info("done.")

    start { zkConnectString =>
      new ExampleMain(Some(zkConnectString)).loadFile(file.getPath)
    }
  }
}