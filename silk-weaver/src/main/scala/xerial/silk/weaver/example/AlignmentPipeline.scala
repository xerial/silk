//--------------------------------------
//
// AlignmentPipeline.scala
// Since: 2013/08/05 12:21 PM
//
//--------------------------------------

package xerial.silk.weaver.example

import xerial.core.log.Logger
import xerial.core.util.DataUnit._

/**
 * Alignment pipeline
 * @param sample sample name
 */
class AlignmentPipeline(sample: String = "HS00001",
            sampleFolder:String = "/data/illumina",
            depthThresholdForIndel:Int = 1000) extends Logger {

  import xerial.silk._

  val chrList = ((1 to 22) ++ Seq("X", "Y")).map(x => s"chr$x.fa")

  // Construct BWT
  def ref = c"curl http://hgdownload.cse.ucsc.edu/goldenPath/hg19/bigZips/chromFa.tar.gz | tar xvz ${chrList} -O".file
  def bwt = c"bwa index -a $ref".memory(6 * GB) && ref

  // Prepare fastq files
  def fastqFiles = c"""find $sampleFolder/$sample -name "*.fastq" """.lines

  // Alignment.
  def sortedBam  = fastqFiles.flatMapWith(bwt) { (fastq, bwt) =>
    val saIndex = c"bwa align -t 8 $bwt $fastq".cpu(8).file
    val sam = c"bwa samse -P $bwt $saIndex $fastq".file
    val bam = c"samtools view -b -S $sam".file
    c"samtools sort -o $bam".file
  }

  //merging alignment results
  def out = "out.bam"
  def mergedBam = c"samtools merge $out ${sortedBam.mkString(" ")}".file

  // SNV call
  def mpileup = c"mpileup -uf $ref -L $depthThresholdForIndel $mergedBam | bcftools view -bvcg -".file
  def snvCall = c"bcftools view $mpileup | vcfutils.pl varFilter -D1000".file


//    // Asign annotation (dbSNP, refseq, OMIM, etc.)
//    for{
//      snv <- snvCall
//      snvWithAnnotation <- snv.join(dbSNP) }  yield ...


}