//--------------------------------------
//
// Make.scala
// Since: 2013/04/16 11:07 AM
//
//--------------------------------------

package xerial.silk.example

import java.io.File
import xerial.silk.core.{file, SilkSingle, Silk}


import xerial.silk._

/**
 * Make example
 * @author Taro L. Saito
 */
object Make {

  def inputFiles = c"""find src -name "*.scala" """.lines
  def wc(file: String) = c"wc -l $file | cut -f 1 -d ' '".lines.head.map(_.trim.toInt)
  def md5sum(file: String) = c"md5sum $file".lines.head.map {
    line =>
      val c = line.split( """\w+""")
      (c(0), c(1)) // md5sum, file name
  }

  def wordCount = for (f <- inputFiles) yield wc(f)
  def md5sumAll = for (f <- inputFiles) yield md5sum(f)
}

object Align {

  case class FastqFile(name: String) {
    val suffix: Option[String] = """_([12])?\.fastq$""".r.findFirstMatchIn(name).map(_.group(0))
    val prefix = name.replaceAll( """(_[12])?\.fastq$""", "")
    val pairFiles = (new File(s"${prefix}_1.fastq"), new File(s"${prefix}_2.fastq"))
  }

  /**
   * Alignment pipeline
   * @param sample sample name
   */
  class Align(sample: String = "HS00001",
              sampleFolder:String = "/data/illumina",
              depthThresholdForIndel:Int = 1000) {

    import xerial.silk._

    val chrList = ((1 to 22) ++ Seq("X", "Y")).map(x => s"chr$x.fa")

    // Construct BWT
    @file(name = "hg19.fa")
    def hg19 = c"curl http://hgdownload.cse.ucsc.edu/goldenPath/hg19/bigZips/chromFa.tar.gz | tar xvz ${chrList} -O"
    def ref = c"bwa index -a ${hg19}" && hg19.file

    def saIndex(fastq: File) = c"bwa align -t 8 $ref $fastq".par
    // Alignment
    def samse(fastq: File) = c"bwa samse -P $ref ${saIndex(fastq)} $fastq"
    def sampe(fastq1: File, fastq2: File) = c"bwa sampe -P $ref ${saIndex(fastq1)} ${saIndex(fastq2)} $fastq1 $fastq2"

    // SAM -> BAM
    def samToBam(sam: File) = c"samtools view -b -S $sam"
    def sortBam(bam: File) = c"samtools sort -o $bam"

    // Pipeline: Alignment -> SAM -> Sorted BAM
    def alignSingleEnd(fastq: File) = samse(fastq) | samToBam | sortBam
    def alignPairedEnd(fastq1: File, fastq2: File) = sampe(fastq1, fastq2) | samToBam | sortBam

    // Input FASTQ files
    def fastqFiles = c"""find $sampleFolder/$sample -name "*.fastq" """.lines.map(FastqFile(_))

    // Perform alignments
    def align = fastqFiles.map { _.pairFiles match { case (p1, p2) => alignPairedEnd(p1, p2) }}

    // Generate a merged BAM
    def mergeBam(bamFiles: Seq[File], out: File) = c"samtools merge $out ${bamFiles.mkString(" ")}" && out
    def mergedBam = mergeBam(align.map(_.file).toSeq, new File("out.bam"))

    // SNV call
    def mpileup(bam:File) = c"mpileup -uf $ref -L $depthThresholdForIndel $bam | bcftools view -bvcg -"
    def snpCall(bcf:File) = c"bcftools view $bcf | vcfutils.pl varFilter -D1000"
    def snpVCF = mpileup(mergedBam.get) | snpCall

  }

}



