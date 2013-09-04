//--------------------------------------
//
// LoadFileTest.scala
// Since: 2013/09/04 10:53 AM
//
//--------------------------------------

package xerial.silk.example

import xerial.silk.cluster.{ClusterUser3Spec, Cluster3Spec}
import java.io.{PrintWriter, BufferedWriter, FileWriter, File}

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

class LoadFileTestMultiJvm3 extends ClusterUser3Spec {
  "parse file" in {

    // Create a temporary file
    info("preparing sample file")
    val file = File.createTempFile("load-test", ".tab", new File("target"))
    file.deleteOnExit()
    val out = new PrintWriter(new BufferedWriter(new FileWriter(file)))
    val N = 1000000
    for(i <- (0 until N)) {
      val p = new Person(i, Person.randomName)
      out.println(p.toTSV)
    }
    out.close()
    info("done.")

    start { zkConnectString =>
      new ExampleMain(zkConnectString).loadFile(file.getPath)
    }
  }
}