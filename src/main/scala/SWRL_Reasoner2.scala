import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}
import Compiler._
import Parser._
import Operator._
import Util._
import scala.io.Source

/**
  * Created by gon on 2016-06-23.
  */
object SWRL_Reasoner2 {
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)


  type Triple = (String, String, String)
  val rdf_type = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    System.setProperty("hadoop.home.dir", "c:\\winutil\\")

    val (compiledRuleSet, predicateMap) = RuleCompiler("RuleSet/0626.txt")
    //    val (compiledRuleSet, predicateMap) = RuleCompiler(args(0))

    val parsedRuleSet = Source.fromFile("RuleSet/0626.txt").getLines().toList.map(RuleParser)
    //    val parsedRuleSet = Source.fromFile(args(0)).getLines().toList.map(RuleParser)

    val ruleDependency_Map = Create_ruleDependency_Map(parsedRuleSet)

    val ruleSet_Size = compiledRuleSet.size
    val triples = sc.textFile("Data/0626.txt").map(parseTriple)
    //    val triples = sc.textFile(args(1)).map(parseTriple)

    var predicateRDD = Create_PredicateRDD(triples, predicateMap)
    //
    //    predicateRDD(0).collect().foreach(println)
    //    predicateRDD(1).collect().foreach(println)

    //    val (deliberationPlan_List, reasoningPlan) = compiledRuleSet
    val deliberationPlan_List = compiledRuleSet.map(t => t._1)
    val applyFunction_List = compiledRuleSet.map(t => t._2)
    val reasoningPlan_List = compiledRuleSet.map(t => t._3)
    //
    //    println("222222222222")
    //    applyFunction_List.foreach(println)

    var deliberationRDD_List: Array[RDD[List[Triple]]] = new Array[RDD[List[Triple]]](ruleSet_Size)
    var oldDeliberationRDD_List: Array[RDD[List[Triple]]] = new Array[RDD[List[Triple]]](ruleSet_Size)

    for (num <- 0 to ruleSet_Size - 1) {
      oldDeliberationRDD_List(num) = sc.emptyRDD
    }
    var conclusionRDD: RDD[Triple] = sc.emptyRDD

    var deliberatedCount: Long = -1
    var phaseCount = 1

    var triggeredRule_List: List[Int] = (0 to ruleSet_Size - 1).toList


    while (deliberatedCount != 0) {
      val phaseTime = System.currentTimeMillis()
      deliberatedCount = 0

      println()
      println("======================Phase" + phaseCount + "======================")


      println("<Deliberation Result>")
      var ruleNum: Int = 0
      val deliberationTime = System.currentTimeMillis()
      for (deliberationPlan <- deliberationPlan_List) {
        //        println(deliberationPlan)
        if (triggeredRule_List.contains(ruleNum)) {
          deliberationRDD_List(ruleNum) = Create_DeliberationRDD(sc, predicateRDD, deliberationPlan, applyFunction_List(ruleNum)).subtract(oldDeliberationRDD_List(ruleNum)).repartition(4) //.persist(StorageLevel.MEMORY_ONLY_SER)
          oldDeliberationRDD_List(ruleNum) = oldDeliberationRDD_List(ruleNum).union(deliberationRDD_List(ruleNum)).repartition(4) //.persist(StorageLevel.MEMORY_ONLY_SER)

          //          deliberationRDD_List(ruleNum).collect().foreach(println)
          val startTime = System.currentTimeMillis()
          val count = deliberationRDD_List(ruleNum).count()
          deliberatedCount += count
          val oldCount = oldDeliberationRDD_List(ruleNum).count()
          val endTime = System.currentTimeMillis()
          println("     Rule " + ruleNum + " : Deliberated = " + count + ", Time = " + (endTime - startTime).toFloat / 1000)


        }
        else {
          println("     Rule " + ruleNum + " : Not deliberated.")
        }
        ruleNum += 1
      }
      val deliberationEndTime = System.currentTimeMillis()
      println("ToTal Deliberation Time : " + (deliberationEndTime - deliberationTime).toFloat / 1000)

      var tmpRule_List: List[Int] = List()
      if (deliberatedCount != 0) {

        println("<Reasoning Result>")
        //          ruleNum = 0
        val reasoningTime = System.currentTimeMillis()
        for (num <- 0 to ruleSet_Size - 1) {
          if (triggeredRule_List.contains(num)) {
            //            val inferredTriples: RDD[Triple] = Reasoning(deliberationRDD_List(num), reasoningPlan_List(num))
            val (inferredTriples, history) = Reasoning(deliberationRDD_List(num), reasoningPlan_List(num))
            val startTime = System.currentTimeMillis()
            val count = inferredTriples.count()
            val endTime = System.currentTimeMillis()

            history.collect().foreach(println)
            //            inferredTriples.collect().foreach(println)
            println("     Rule " + num + " : Inferred = " + count + ", Time = " + (endTime - startTime).toFloat / 1000)

            val p = if (reasoningPlan_List(num)(1)._2 == rdf_type) {
              reasoningPlan_List(num)(2)._2
            } else {
              reasoningPlan_List(num)(1)._2
            }
            val predIndex = predicateMap(p)
            predicateRDD(predIndex) = predicateRDD(predIndex).union(inferredTriples).distinct().repartition(4) //.persist(StorageLevel.MEMORY_ONLY_SER)
//            val realInferred = predicateRDD(predIndex)
            //List( (s, p, o), (s,p, o), ...)
            //            val saveFile = deliberationRDD_List(num).map(t => (DeliberationToString(t), real)
            if (count > 0) {
              tmpRule_List ++= ruleDependency_Map.get(num).getOrElse(List())
            }

            //      inferredTriples.collect().foreach(println)
          }
          //            ruleNum += 1
        }
        val reasoningEndTime = System.currentTimeMillis()
        println("ToTal Deliberation Time : " + (reasoningEndTime - reasoningTime).toFloat / 1000)

      }
      else {
        println("<This phase has no deliberated rules>")
      }

      triggeredRule_List = tmpRule_List.distinct
      println("Triggered Rule List : " + triggeredRule_List)


      val endPhaseTime = System.currentTimeMillis()
      println("Phase " + phaseCount + " Duration = " + (endPhaseTime - phaseTime).toFloat / 1000)
      phaseCount += 1
    }

  }
}