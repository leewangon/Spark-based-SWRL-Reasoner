import Util._
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import Compiler._
import org.apache.spark.storage.StorageLevel

import scala.util.control.Breaks._

/**
  * Created by Gon on 2016-06-19.
  */
object Operator {
  type Triple = (String, String, String)
  val rdf_type = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"

  def Create_PredicateRDD(triples: RDD[Triple], predicateMap: Map[String, Int]) = {
    val sizeOfPredicateMap = predicateMap.size
    val atomRDD_List: Array[RDD[Triple]] = new Array[RDD[Triple]](sizeOfPredicateMap)

    for ((pred, idx) <- predicateMap.toList) {
      atomRDD_List(idx) = triples.filter { case (Triple(s, p, o)) => (p == rdf_type && o == pred) || p == pred }.persist(StorageLevel.MEMORY_ONLY_SER)
      //      println("pred"+pred)
      //      atomRDD_List(idx).collect().foreach(println)
    }

    atomRDD_List
  }

  def Update_conditionRDD(conditionRDD: RDD[Triple], inferred: RDD[Triple]): (RDD[Triple], Boolean) = {
    var flag: Boolean = false
    if (inferred.subtract(conditionRDD).count() > 0) {
      //      println("condition RDD")
      //      conditionRDD.collect().foreach(println)
      //      println("inferred RDD")
      //      inferred.collect().foreach(println)
      //      println("subtract count : "+conditionRDD.subtract(inferred).count())
      flag = true
      (conditionRDD.union(inferred).distinct(), flag)
    }
    else {
      (conditionRDD, flag)
    }


  }

  def Join_RDD(leftRDD: RDD[List[Triple]], rightRDD: RDD[Triple], leftIndex: List[Int], rightIndex: Int) = {
    val conditionIndex = leftIndex(0)
    val variableIndex = leftIndex(1)

    val leftPair_RDD = leftRDD.keyBy(_ (conditionIndex).productElement(variableIndex).toString)
    //      .map { conditionList =>
    //      val key = conditionList(conditionIndex).productElement(variableIndex).toString
    //      (key, List(_))
    //    }
    val rightPair_RDD = rightRDD.map { case (triple) => (triple.productElement(rightIndex).toString, List(triple)) }

    leftPair_RDD.join(rightPair_RDD)
      .map { case ((joinKey, (leftTripleList, rightTripleList))) => leftTripleList ++ rightTripleList }
  }

  def Reasoning(deliberationRDD: RDD[List[Triple]], reasoningPlan: List[(List[Int], String)]) = {
    val subj = reasoningPlan(0)._2
    val subjIndex = reasoningPlan(0)._1
    val pred = reasoningPlan(1)._2
    val obj = reasoningPlan(2)._2
    val objIndex = reasoningPlan(2)._1

    if (subj.contains("?") && obj.contains("?")) {
      //      println("here")
      //      reasoningPlan.foreach(println)
      //      deliberationRDD.collect().foreach(println)
      val inferredTriples = deliberationRDD.map(tripleList => (tripleList(subjIndex(0)).productElement(subjIndex(1)).toString, pred, tripleList(objIndex(0)).productElement(objIndex(1)).toString))
      val history = deliberationRDD.map(tripleList => (tripleList, ConclusionToString((tripleList(subjIndex(0)).productElement(subjIndex(1)).toString, pred.replace(rdf_type, "type"), tripleList(objIndex(0)).productElement(objIndex(1)).toString))))
      (inferredTriples, history)
    }
    else if (subj.contains("?") && !obj.contains("?")) {
      val inferredTriples = deliberationRDD.map(tripleList => (tripleList(subjIndex(0)).productElement(subjIndex(1)).toString, pred, obj))
      val history = deliberationRDD.map(tripleList => (tripleList, ConclusionToString((tripleList(subjIndex(0)).productElement(subjIndex(1)).toString, pred.replace(rdf_type, "type"), obj))))
        (inferredTriples, history)
    }
    else {
      //      deliberationRDD.collect().foreach(println)
      val inferredTriples = deliberationRDD.map(tripleList => (subj, pred, tripleList(objIndex(0)).productElement(objIndex(1)).toString))
      val history = deliberationRDD.map(tripleList => (tripleList, ConclusionToString((subj, pred.replace(rdf_type, "type"), tripleList(objIndex(0)).productElement(objIndex(1)).toString))))

      (inferredTriples, history)
    }
  }

  //
  //  def Reasoning(ruleRDD: RDD[List[Triple]], condition: List[Triple], conclusion: Triple): RDD[Triple] = {
  //    val (subjectIndex, predicate, objectIndex) = ConclusionIndex_Finder(condition, conclusion)
  //
  //    if (objectIndex.nonEmpty) {
  //      val tripleIndex_forSubj = subjectIndex(0)
  //      val commonVarIndex_forSubj = subjectIndex(1)
  //
  //      val tripleIndex_forObj = objectIndex(0)
  //      val commonVarIndex_forObj = objectIndex(1)
  //
  //      val conclusionRDD = ruleRDD.map { tripleList =>
  //        val subj = if (tripleIndex_forSubj == -1) {
  //          conclusion._1
  //        } else {
  //          tripleList(tripleIndex_forSubj).productElement(commonVarIndex_forSubj).toString
  //        }
  //        //        val subj = tripleList(tripleIndex_forSubj).productElement(commonVarIndex_forSubj).toString
  //        val pred = predicate
  //        val obj = if (tripleIndex_forObj == -1) {
  //          conclusion._3
  //        } else {
  //          tripleList(tripleIndex_forObj).productElement(commonVarIndex_forObj).toString
  //        }
  //        //        val obj =  tripleList(tripleIndex_forObj).productElement(commonVarIndex_forObj).toString
  //
  //        (subj, pred, obj)
  //      }
  //      conclusionRDD
  //    }
  //    else {
  //      val tripleIndex_forSubj = subjectIndex(0)
  //      val commonVarIndex_forSubj = subjectIndex(1)
  //
  //      val conclusionRDD = ruleRDD.map { tripleList =>
  //        val subj = tripleList(tripleIndex_forSubj).productElement(commonVarIndex_forSubj).toString
  //        val pred = rdf_type
  //        val obj = predicate
  //
  //        (subj, pred, obj)
  //      }
  //      conclusionRDD
  //
  //    }
  //  }

  def Filter_RDD(conditionRDD: RDD[Triple], condition: Triple): RDD[Triple] = {
    val (s, p, o) = condition

    if (!s.contains("?")) {
      //subject has no "?" means that it is not variable but value
      conditionRDD.filter(_._1 == s)
    }
    else if (!o.contains("?")) {
      conditionRDD.filter(_._3 == o)
    }
    else {
      conditionRDD
    }
  }

  def Create_DeliberationRDD(sc: SparkContext, predicateRDD: Array[RDD[Triple]], deliberationTask_List: List[Deliberation], applyFunction_List: List[(String, List[Int], List[Int], Int)]) = {
    var left: RDD[(String, List[Triple])] = sc.emptyRDD
    var right: RDD[(String, List[Triple])] = sc.emptyRDD
    var joinResult: RDD[List[Triple]] = sc.emptyRDD
    var deliberationRDD: Array[RDD[List[Triple]]] = new Array[RDD[List[Triple]]](100)

    //("Filter", Map("predID" -> predID, "keyIndex" -> commonVariable_Index, "filterCondition" -> List(("s", s), ("p", p), ("o", o))))

    if (deliberationTask_List(0)._1 == "singDeliberation") {
      val singleFilter = deliberationTask_List(0)._2._1
      val predID = singleFilter._2
      val filterCondition = singleFilter._4

      joinResult = predicateRDD(predID).filter { case ((s, p, o)) =>
        if (filterCondition.size == 3) {
          s == filterCondition.head._2 && p == filterCondition(1)._2 && o == filterCondition(2)._2
        } else if (filterCondition.size == 1 && filterCondition.head._1 == "s") {
          s == filterCondition.head._2
        } else if (filterCondition.size == 1 && filterCondition.head._1 == "o") {
          o == filterCondition.head._2
        } else {
          true
        }
      }.map(t => List(t))

    }
    else {

      for (task <- deliberationTask_List) {
        val (filterLeft, filterRight) = task._2
        filterLeft._1 match {
          case "Filter" =>
            val predID = filterLeft._2
            val keyIndex = filterLeft._3
            val filterCondition = filterLeft._4

            left = predicateRDD(predID).filter { case ((s, p, o)) =>
              if (filterCondition.size == 3) {
                s == filterCondition.head._2 && p == filterCondition(1)._2 && o == filterCondition(2)._2
              } else if (filterCondition.size == 1 && filterCondition.head._1 == "s") {
                s == filterCondition.head._2
              } else if (filterCondition.size == 1 && filterCondition.head._1 == "o") {
                o == filterCondition.head._2
              } else {
                true
              }
            }.map { case (triple) => (triple.productElement(keyIndex(1)).toString, List(triple)) }

          case "BeforeRDD" =>
            val predID = filterLeft._2
            val keyIndex = filterLeft._3
            val filterCondition = filterLeft._4

            left = joinResult.map(tripleList => (tripleList(keyIndex(0)).productElement(keyIndex(1)).toString, tripleList))

        }

        filterRight._1 match {
          case "Filter" =>
            val predID = filterRight._2
            val keyIndex = filterRight._3
            val filterCondition = filterRight._4


            right = predicateRDD(predID).filter { case ((s, p, o)) =>
              if (filterCondition.size == 3) {
                s == filterCondition.head._2 && p == filterCondition(1)._2 && o == filterCondition(2)._2
              } else if (filterCondition.size == 1 && filterCondition.head._1 == "s") {
                s == filterCondition.head._2
              } else if (filterCondition.size == 1 && filterCondition.head._1 == "o") {
                o == filterCondition.head._2
              } else {
                true
              }
            }.map { case (triple) => (triple.productElement(keyIndex(1)).toString, List(triple)) }

        }


        //        println("left")
        //        left.collect().foreach(println)
        //        println("right")
        //        right.collect().foreach(println)
        left = left.partitionBy(new HashPartitioner(300))
        right = right.partitionBy(new HashPartitioner(300))
        joinResult = left.join(right).map { case ((joinKey, (leftTripleList, rightTripleList))) => leftTripleList ++ rightTripleList }


      }


    }



    if (applyFunction_List.nonEmpty) {
      for (functionCond <- applyFunction_List) {
        functionCond._1 match {
          case "<lessthanorequal>" => {
            val idx1 = functionCond._2
            val idx2 = functionCond._3
            val value = functionCond._4
            if (idx1.nonEmpty && idx2.nonEmpty) {
              joinResult = joinResult.filter(tripleList => tripleList(idx1(0)).productElement(idx1(1)).toString.replaceAll("<|>|\"", "").toInt >= tripleList(idx2(0)).productElement(idx2(1)).toString.replaceAll("<|>", "").toInt)
            }
            else if (idx1.nonEmpty) {
              joinResult = joinResult.filter(tripleList => tripleList(idx1(0)).productElement(idx1(1)).toString.replaceAll("<|>|\"", "").toInt >= value)
            }
            else if (idx2.nonEmpty) {
              joinResult = joinResult.filter(tripleList => value >= tripleList(idx2(0)).productElement(idx2(1)).toString.replaceAll("<|>|\"", "").toInt)
            }
          }
          case "<equal>" => {
            val idx1 = functionCond._2
            val idx2 = functionCond._3
            val value = functionCond._4
            if (idx1.nonEmpty && idx2.nonEmpty) {
              joinResult = joinResult.filter(tripleList => tripleList(idx1(0)).productElement(idx1(1)).toString.replaceAll("<|>|\"", "").toInt == tripleList(idx2(0)).productElement(idx2(1)).toString.replaceAll("<|>", "").toInt)
            }
            else if (idx1.nonEmpty) {
              joinResult = joinResult.filter(tripleList => tripleList(idx1(0)).productElement(idx1(1)).toString.replaceAll("<|>|\"", "").toInt == value)
            }
            else if (idx2.nonEmpty) {
              joinResult = joinResult.filter(tripleList => value == tripleList(idx2(0)).productElement(idx2(1)).toString.replaceAll("<|>|\"", "").toInt)
            }
          }
          case _ => joinResult

        }
      }
    }

    joinResult


  }


  //  def Create_ConditionRDD(sc:SparkContext, rule:(List[Triple], Triple), predicate_Map:Map[String, Int], conditionRDD:Array[RDD[Triple]]) ={
  //    var conditionList: List[Triple] = rule._1
  //    val conclusion: Triple = rule._2
  //
  //    val firstCondition: Triple = conditionList.head
  //    conditionList = conditionList.drop(1)
  //
  //    var leftCondition: List[Triple] = null
  //    var rightCondition: Triple = null
  //    var joinResult_Condition: List[Triple] = List(firstCondition)
  //
  //    var leftRDD: RDD[List[Triple]] = sc.emptyRDD
  //    var rightRDD: RDD[Triple] = sc.emptyRDD
  //
  //    val firstCondition_ID = Predicate_ID_Finder(predicate_Map, firstCondition)
  //
  //    var joinResult_RDD: RDD[List[Triple]] = Filter_RDD(conditionRDD(firstCondition_ID), firstCondition).map(List(_))
  ////    var joinResult_RDD: RDD[List[Triple]] = conditionRDD(firstCondition_ID).map(List(_))
  //    //var rule_RDD_List:Array[RDD[List[Triple]]] = new Array[RDD[List[Triple]]](sizeOf_Rules)
  //
  //
  //    while (conditionList.nonEmpty) {
  //      leftCondition = joinResult_Condition
  //      leftRDD = joinResult_RDD
  //
  //      breakable {
  //        for (rightCondition <- conditionList) {
  //          val (leftIndex, rightIndex) = CommonVariable_Finder(leftCondition, rightCondition)
  //
  //          if (leftIndex.nonEmpty) {
  //            val rightCondition_ID = Predicate_ID_Finder(predicate_Map, rightCondition)
  //            rightRDD = Filter_RDD(conditionRDD(rightCondition_ID), rightCondition)
  //
  //            joinResult_RDD = Join_RDD(leftRDD, rightRDD, leftIndex, rightIndex)
  //
  //            //              leftRDD.collect().foreach(println)
  //            //              println("***********")
  //            //              rightRDD.collect().foreach(println)
  //
  //
  //            joinResult_Condition = leftCondition ++ List(rightCondition)
  //            conditionList = Remove(rightCondition, conditionList)
  //
  //            break
  //          }
  //        }
  //      }
  //    }
  //    (joinResult_RDD, joinResult_Condition)
  //  }


}
