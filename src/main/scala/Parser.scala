

/**
  * Created by Gon on 2016-06-19.
  */
object Parser {
  val xbp = "http://xb.saltlux.com/schema/property/"
  val xbc = "http://xb.saltlux.com/schema/class/"
  val xbr = "http://xb.saltlux.com/resource/"
  val rdf_type = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"


  type Triple = (String, String, String)

  def ReplacePrefix(input:String) : String={
    val prefixList = List(("xbp:", xbp), ("xbc:", xbc), ("xbr:", xbr))
    var line : String = input
    for(prefix <- prefixList ){
      line = line.replaceAll(prefix._1, prefix._2)
    }
    line
  }

  def SetAngle(line:String):String = {
    "<"+line+">"
  }

  def AtomToTriple(atom:Array[String]) : Triple = {
    if(atom.size == 2){
      (atom(1), rdf_type, atom(0))
    }
    else{
      (atom(1), atom(0), atom(2))
    }

  }

  def RuleParser(rule:String) : (List[Triple], Triple)={
    val parsedRule = ReplacePrefix(rule).split("\\^|->").map(_.trim)
                               .map(atom => atom.split("[(),\\s]+").map(SetAngle))
                               .map(AtomToTriple)

    val conditionList = parsedRule.dropRight(1).toList
    val conclusion = parsedRule.last

    (conditionList, conclusion)
  }


  def parseTriple(triple: String) : Triple = {
    // Get subject
    val subj = if (triple.startsWith("<")) {
      triple.substring(0, triple.indexOf('>') + 1)
    } else {
      triple.substring(0, triple.indexOf(' '))
    }

    // Get predicate
    val triple0 = triple.substring(triple.indexOf(' ') + 1)
    val pred = triple0.substring(0, triple0.indexOf('>') + 1)

    val triple1 = triple0.substring(pred.length + 1)

    // Get object
    val obj = if (triple1.startsWith("<")) {
      triple1.substring(0, triple1.indexOf('>') + 1)
    }
    else if (triple1.startsWith("\"")) {
      triple1.substring(0, triple1.substring(1).indexOf('\"') + 2)
    }
    else {
      triple1.substring(0, triple1.indexOf(' '))
    }

    Triple(subj, pred, obj)
  }


}
