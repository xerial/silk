package xerial.silk.glens

//--------------------------------------
//
// IUPAC.scala
// Since: 2012/03/16 11:25
//
//--------------------------------------

/**
 * IUPAC code of DNA alleles
 * @author leo
 */
object IUPAC {
  object Empty extends IUPAC("*", "*", 0x00)
  object A extends IUPAC("A", "A", 0x01)
  object C extends IUPAC("C", "C", 0x02)
  object G extends IUPAC("G", "G", 0x04)
  object T extends IUPAC("T", "T", 0x08)
  object M extends IUPAC("M", "A/C", 0x03)
  object R extends IUPAC("R", "A/G", 0x05)
  object W extends IUPAC("W", "A/T", 0x09)
  object S extends IUPAC("S", "C/G", 0x06)
  object Y extends IUPAC("Y", "C/T", 0x0A)
  object K extends IUPAC("K", "G/T", 0x0C)
  object V extends IUPAC("V", "A/C/G", 0x07)
  object H extends IUPAC("H", "A/C/T", 0x0B)
  object D extends IUPAC("D", "A/G/T", 0x0D)
  object B extends IUPAC("B", "C/G/T", 0x0E)
  object N extends IUPAC("N", "A/C/G/T", 0x0F)

  val values = Array(Empty, A, C, G, T, M, R, W, S, Y, K, V, H, D, B, N)

  private[glens] val bitFlagToIUPACTable = {
    val table = new Array[IUPAC](values.length)
    for(each <- values) {
      table(each.bitFlag & 0x0F) = each
    }
    table
  }

  private[glens] lazy val symbolTable = {
    values.map(each => each.symbol -> each).toMap
  }

  private[glens] val complementTable = Array[IUPAC](Empty, T, G, K, C, Y, S, B, A, W,
    R, D, M, H, V, N)


  def genoTypetoIUPAC(genoType:String) : IUPAC = {
    val flag = genoType.foldLeft(0){(flag, ch) =>
      val code = DNA.to2bitCode(ch)
      flag | (1 << code)
    }
    bitFlagToIUPACTable(flag & 0x0F)
  }

  def toIUPAC(symbol:String) : Option[IUPAC] = symbolTable.get(symbol)

}

sealed abstract class IUPAC(val symbol:String,val variation:String, val bitFlag:Int) extends GenomeLetter {

  def complement : IUPAC = IUPAC.complementTable(bitFlag)

  lazy val genoType : String = {
    val genoType = new StringBuilder(4)
    def loop(index:Int) {
      val flag = 1 << index
      if(index < 4) {
        if((bitFlag & flag) != 0)
          genoType += DNA.decode(index.toByte).toChar
        loop(index+1)
      }
    }
    loop(0)
    genoType.result
  }


}