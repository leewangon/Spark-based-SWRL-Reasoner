import Parser._
import Util._

import scala.io.Source
import scala.util.control.Breaks._

/**
  * Created by gon on 2016-06-23.
  */
object Compiler {
  type Triple = (String, String, String)
  val rdf_type: String = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
  type Filter = (String, Int, List[Int], List[(String, String)])
  type Deliberation = (String, (Filter, Filter))
  //  type Inferrence

  def RuleCompiler(filePath: String) = {
    val parsedRuleSet = Source.fromFile(filePath).getLines().toList.map(RuleParser).toList
    //(conditionList, conclusion)

    val predicateMap = Create_PredicateMap(parsedRuleSet)

    val compiledRuleSet = parsedRuleSet.map { case ((conditionList, conclusion)) =>
      //      var compiledRule = List[Task]

      //      val (deliberationTask_List, groupedCondition) = DeliberationTask_Builder(conditionList, predicateMap)
      val (deliberationPlan, applyFunction_List) = DeliberationPlan_Builder(conditionList, predicateMap)
      val reasoningPlan = ReasoningPlan_Builder(conditionList, conclusion)
      (deliberationPlan, applyFunction_List, reasoningPlan)
    }
    (compiledRuleSet, predicateMap)
  }

  def ReasoningPlan_Builder(conditionList: List[Triple], conclusion: Triple) = {
    val subj: String = conclusion._1
    val pred: String = conclusion._2
//      if(conclusion._2 == rdf_type){
//      conclusion._3}
//    else{
//      conclusion._2
//    }


    val obj: String = conclusion._3
    var subjIndex: List[Int] = List()
    var predIndex: List[Int] = List()
    var objIndex: List[Int] = List()

    if (subj.contains("?")) {
      subjIndex = CommonVariable_Finder(conditionList, (subj, "", ""))._1
    }

    if (obj.contains("?")) {
      objIndex = CommonVariable_Finder(conditionList, ("", "", obj))._1
    }

    List((subjIndex,subj), (predIndex, pred),(objIndex, obj))
//    List(("Subject", Map("subjIndex" -> subjIndex, "subj" -> subj)), ("Predicate", Map("predIndex" -> subjIndex, "pred" -> subj)), ("Object", Map("objIndex" -> objIndex, "obj" -> obj)))


    //("Filter", predID, commonVariable_Index, List(("s", s), ("p", p), ("o", o)))
    //("Subject", subjectIndex, subject), ("Predicate", predicate), ("Object", object)

  }


  def DeliberationPlan_Builder(condList: List[Triple], predicateMap: Map[String, Int]) = {


    var conditionList = condList
    var functionCondition_List:List[Triple] = List()
    val funcList = List("<lessthanorequal>", "<equal>")
    for(cond <- condList){
      if(funcList.contains(cond._2)){
        functionCondition_List ++= List(cond)
        conditionList = Remove(cond, conditionList)
      }
    }

    val firstCondition: List[Triple] = List(conditionList.head)
    var otherConditionList = conditionList.drop(1)
    var groupedCondition = firstCondition
    var deliberationTask_List: List[Deliberation] = List()
    var leftPred_ID = 0
    var rightPred_ID = 0


    if(condList.size == 1){
      //List( ("Single", (("SingleFilter", predicateMap.get(condList(0)._2).get, List(), List()), filter)))
      val f1 = FilterOP_Generator(firstCondition, predicateMap.get(condList(0)._2).get, List(-1))
      val f2 = FilterOP_Generator(firstCondition, predicateMap.get(condList(0)._2).get, List(-1))
      deliberationTask_List ++= List(("singDeliberation", (f1, f2)))
    }
    else {
      while (otherConditionList.nonEmpty) {
        val leftCondition = groupedCondition

        breakable {
          for (rightCondition <- otherConditionList) {

            val (leftIndex, rightIndex) = CommonVariable_Finder(leftCondition, rightCondition)


            if (leftIndex.nonEmpty) {
              leftPred_ID = PredicateID_Finder(leftCondition, predicateMap)
              rightPred_ID = PredicateID_Finder(List(rightCondition), predicateMap)

              leftCondition match {
                //first deliberation case
                case firstCondition =>
                  val leftFilter = FilterOP_Generator(leftCondition, leftPred_ID, leftIndex)
                  val rightFilter = FilterOP_Generator(List(rightCondition), rightPred_ID, rightIndex)
                  deliberationTask_List ++= List(("Deliberation", (leftFilter, rightFilter)))
                //

                //n-th deliberation case
                case _ =>
                  val leftFilter = FilterOP_Generator(leftCondition, leftPred_ID, leftIndex)
                  val rightFilter = FilterOP_Generator(List(rightCondition), rightPred_ID, rightIndex)
                  deliberationTask_List ++= List(("Deliberation", (leftFilter, rightFilter)))
              }

              otherConditionList = Remove(rightCondition, otherConditionList)
              groupedCondition = leftCondition ++ List(rightCondition)
              break
            }
          }
        }
      }
    }
//    deliberationTask_List.foreach(println)

    var applyFunction_List:List[(String, List[Int], List[Int], Int)] = List()
    for(functionCondition <- functionCondition_List){
      if(functionCondition._1.contains("?") && functionCondition._3.contains("?")){
        val (idx1, r1Index) = CommonVariable_Finder(groupedCondition, (functionCondition._1, "", ""))
        val (idx2, r2Index) = CommonVariable_Finder(groupedCondition, ("", "", functionCondition._3))
        applyFunction_List ++= List((functionCondition._2, idx1, idx2, functionCondition._3.replaceAll("<|>|\"", "").toInt))
      }
      else if(functionCondition._1.contains("?")&& !functionCondition._3.contains("?")){
        val (leftIndex, rightIndex) = CommonVariable_Finder(groupedCondition, functionCondition)
        applyFunction_List ++= List((functionCondition._2, leftIndex, List(), functionCondition._3.replaceAll("<|>|\"", "").toInt))
      }
      else{
        val (leftIndex, rightIndex) = CommonVariable_Finder(groupedCondition, functionCondition)
        applyFunction_List ++= List((functionCondition._2, List(),leftIndex, functionCondition._1.replaceAll("<|>|\"", "").toInt))
      }
    }

//    applyFunction_List.foreach(println)

    //    groupedCondition.foreach(println)
    (deliberationTask_List, applyFunction_List) //, groupedCondition)

  }

  def FilterOP_Generator(condition: List[Triple], predID: Int, commonVariable_Index: List[Int]) = {
    if (condition.size == 1) {
      val (s, p, o) = (condition(0)._1, condition(0)._2, condition(0)._3)

      if (!s.contains("?") && p == rdf_type) {
        //        ("Filter", Map("predID" -> predID, "keyIndex" -> commonVariable_Index, "filterCondition" -> List(("s", s), ("p", p), ("o", o))))
        ("Filter", predID, commonVariable_Index, List(("s", s), ("p", p), ("o", o)))
      }
      else if (s.contains("?") && o.contains("?")) {
        //        ("Filter", Map("predID" -> predID, "keyIndex" -> commonVariable_Index))
        ("Filter", predID, commonVariable_Index, List())
      }
      else if (s.contains("?")) {
        //        ("Filter", Map("predID" -> predID, "keyIndex" -> commonVariable_Index, "filterCondition" -> List(("o", o))))
        ("Filter", predID, commonVariable_Index, List(("o", o)))
      }
      else {
        //xbp:hasParent(?b, ?f)
        //        ("Filter", Map("predID" -> predID, "keyIndex" -> commonVariable_Index, "filterCondition" -> List(("s", s))))
        ("Filter", predID, commonVariable_Index, List(("s", s)))
      }

    }
    else {
      //      ("BeforeRDD", Map("keyIndex" -> commonVariable_Index))
      ("BeforeRDD", predID, commonVariable_Index, List())
    }


  }


}
