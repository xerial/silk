//--------------------------------------
//
// Make.scala
// Since: 2013/04/16 11:07 AM
//
//--------------------------------------

package xerial.silk.example


import xerial.silk._
import xerial.core.log.Logger



/**
 * Make example
 * @author Taro L. Saito
 */
object Make {


  def searchFiles = c"""find src -name "*.scala" """
  def inputFiles = searchFiles.lines
  def wc(file: String) = {
    val wccmd = c"wc -l $file | cut -f 1 -d ' '"
    wccmd.lines.head.map(_.trim.toInt)
  }
  def md5sum(file: String) = c"md5sum $file".lines.head.map {
    line =>
      val c = line.split( """\w+""")
      (c(0), c(1)) // md5sum, file name
  }

  def wordCount = for (f <- inputFiles) yield wc(f)
  def md5sumAll = for (f <- inputFiles) yield md5sum(f)
}


/**
 * Alignment pipeline
 * @param sample sample name
 */
class Align(sample: String = "HS00001",
            sampleFolder:String = "/data/illumina",
            depthThresholdForIndel:Int = 1000) extends Logger {

  import xerial.silk._

  val chrList = ((1 to 22) ++ Seq("X", "Y")).map(x => s"chr$x.fa")

  // Construct BWT
  def ref = {
    for(hg19 <- c"curl http://hgdownload.cse.ucsc.edu/goldenPath/hg19/bigZips/chromFa.tar.gz | tar xvz ${chrList} -O") yield
      c"bwa index -a $hg19" && hg19.file
  }

  def pipeline = {
    // Prepare fastq files
    val fastqFiles = c"""find $sampleFolder/$sample -name "*.fastq" """


    // alignment
    val sortedBam = for{
      fastq  <- fastqFiles.lines
      saIndex <- c"bwa align -t 8 $ref $fastq".file
      sam <- c"bwa samse -P $ref $saIndex $fastq".file
      bam <- c"samtools view -b -S $sam".file
      sorted <- c"samtools sort -o $bam".file
    } yield sorted

    debug(sortedBam)


    // merging alignment results
    val out = "out.bam"
    val mergedBam = c"samtools merge $out ${sortedBam.mkString(" ")}".file


    // SNV call
    val snvCall = for{
      mpileup <- c"mpileup -uf $ref -L $depthThresholdForIndel $mergedBam | bcftools view -bvcg -".file
      snvCall <- c"bcftools view $mpileup | vcfutils.pl varFilter -D1000".file
    } yield snvCall

    snvCall

//    // Asign annotation (dbSNP, refseq, OMIM, etc.)
//    for{
//      snv <- snvCall
//      snvWithAnnotation <- snv.join(dbSNP) }  yield ...




  }


}








































