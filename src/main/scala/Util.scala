import scala.util.control.Breaks._

/**
  * Created by Gon on 2016-06-19.
  */
object Util {
  type Triple = (String, String, String)
  val xbc = "http://xb.saltlux.com/schema/class/"
  val rdf_type = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"


  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println(math.round(((t1 - t0).toFloat / 1000000000) * 100d) / 100d + "s")
    result
  }

  def Remove[A](i: A, li: List[A]) = {
    val (head, _ :: tail) = li.span(i != _)
    head ::: tail
  }

  def Create_PredicateMap(parsedRules: List[(List[Triple], Triple)]): Map[String, Int] = {
    val conditionPredicate = parsedRules.flatMap { case ((conditionList, conclusion)) => conditionList }
      .map { case ((s, p, o)) => if (p == rdf_type) {
        o
      } else {
        p
      }
      }

    val conclusionPredicate = parsedRules.map(_._2).map { case ((s, p, o)) => if (p == rdf_type) {
      o
    } else {
      p
    }
    }

    val predicateMap = (conditionPredicate ++ conclusionPredicate).distinct.zipWithIndex.toMap
    predicateMap
  }

  def TripleToPredicate(triple: Triple): String = {
    if (triple._2 == "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>") {
      triple._3
    }
    else {
      triple._2
    }
  }


  //: Map[Int, List[Int]]
  def Create_ruleDependency_Map(parsedRules: List[(List[Triple], Triple)]): Map[Int, List[Int]] = {
    val conditionList = parsedRules.map { case ((condition, conclusion)) => condition.map(TripleToPredicate) }
    val conclusionList = parsedRules.map { case ((condition, conclusion)) => TripleToPredicate(conclusion) }

    var conclusion_idx = 0

    //    var ruleDependency_Map : Map[Int, List[Int]] = null
    var ruleDependency_Map: Map[Int, List[Int]] = Map()

    for (conclusion <- conclusionList) {
      var tmp: List[Int] = List()
      var condition_idx = 0
      for (condition <- conditionList) {
        if (condition.contains(conclusion)) {
          tmp = tmp ++ List(condition_idx)
        }
        condition_idx += 1
      }
      if (tmp.nonEmpty) {
        ruleDependency_Map += (conclusion_idx -> tmp)
      }
      conclusion_idx += 1
    }
    //    ruleDependency_Map.foreach(println)
    ruleDependency_Map
  }

  def Predicate_ID_Finder(predicate_Map: Map[String, Int], atom: Triple): Int = {
    if (atom._2 == rdf_type) {
      predicate_Map.get(atom._3).get
    }
    else {
      predicate_Map.get(atom._2).get
    }
  }

  def PredicateID_Finder(conditionList: List[Triple], predicateMap: Map[String, Int]) = {
    if (conditionList.size == 1) {
      if (conditionList(0)._2 == rdf_type) {
        predicateMap.get(conditionList(0)._3).get
      }
      else {
        predicateMap.get(conditionList(0)._2).get
      }

      if (conditionList(0)._2 == rdf_type) {
        predicateMap.get(conditionList(0)._3).get
      }
      else {
        predicateMap.get(conditionList(0)._2).get
      }
    }
    else {
      -1
    }
  }

  def CommonVariable_Finder(leftCondition_List: List[Triple], rightCondition: Triple): (List[Int], List[Int]) = {
    //leftCondition = List[ (?b, hasParent, ?f), (?b, type, Person)], rightCondition =  List[ (?a, hasChild, ?b) ]
    val leftList = leftCondition_List.map { case ((s, p, o)) => List(s, p, o) }
    val right = List(rightCondition._1, rightCondition._2, rightCondition._3)

    var commonVariable: String = null
    for (condition <- leftList) {
      val tmp = condition.intersect(right).filter( t => t.contains("?"))
      if (tmp.nonEmpty) {
        commonVariable = tmp.head
      }
    }

    var conditionIndex = 0
    var left_Index = List[Int]()

    for (left <- leftList) {
      val variableIndex = left.indexOf(commonVariable)
      if (variableIndex != -1) {
        left_Index = List(conditionIndex, variableIndex)
      }
      conditionIndex += 1
    }

    val right_Index = List((0), (right.indexOf(commonVariable)))

    (left_Index, right_Index)

  }

  def ConclusionIndex_Finder(condition: List[Triple], conclusion: Triple): (List[Int], String, List[Int]) = {
    val conditionList = condition.map { case ((s, p, o)) => List(s, p, o) }

    var subject_Index = List[Int]()
    var predicate: String = null
    var object_Index = List[Int]()

    if (conclusion._2 == rdf_type) {
      var conditionIndex = 0
      breakable {
        for (condition <- conditionList) {
          val commonVariableList = condition.intersect(List(conclusion._1))

          if (commonVariableList.nonEmpty) {
            val variableIndex = condition.indexOf(commonVariableList.head)
            subject_Index = List(conditionIndex, variableIndex)
            predicate = conclusion._3
            break
          }
          conditionIndex += 1
        }
      }
    }
    else {
      var conditionIndex = 0
      breakable {
        for (condition <- conditionList) {
          val commonVariableList = condition.intersect(List(conclusion._1))

          if (commonVariableList.nonEmpty) {
            val variableIndex = condition.indexOf(commonVariableList.head)
            subject_Index = List(conditionIndex, variableIndex)
            break
          }
          conditionIndex += 1
        }
      }

      predicate = conclusion._2

      conditionIndex = 0
      breakable {
        for (condition <- conditionList) {
          val commonVariableList = condition.intersect(List(conclusion._3))

          if (commonVariableList.nonEmpty) {
            val variableIndex = condition.indexOf(commonVariableList.head)
            object_Index = List(conditionIndex, variableIndex)
            break
          }
          conditionIndex += 1
        }
      }

      if (subject_Index.isEmpty) {
        subject_Index = List(-1, 0)
      }

      if (object_Index.isEmpty) {
        object_Index = List(-1, 2)
      }
    }



    (subject_Index, predicate, object_Index)
  }

  def DeliberationToString(input:List[Triple])={
    var tmp:List[String]= List()
    for(t <- input){
      if(t._2 == rdf_type){
        val nt = (t._1, "type", t._3)
        tmp ++= List(nt.toString().replaceAll(",", " "))
      }
      tmp ++= List(t.toString().replaceAll(",", " "))

    }
    tmp
  }

  def ConclusionToString(input:Triple)={
      if(input._2 == rdf_type){
        (input._1, "type", input._3).toString().replaceAll(",", " ")
      }
    else{
        input.toString().replaceAll(",", " ")
      }

  }

}
