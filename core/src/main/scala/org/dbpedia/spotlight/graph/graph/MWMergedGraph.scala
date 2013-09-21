package org.dbpedia.spotlight.graph

import it.unimi.dsi.webgraph.labelling.ArcLabelledImmutableGraph
import org.dbpedia.spotlight.exceptions.InitializationException
import collection.mutable
import java.io.{File, PrintStream, FileOutputStream, OutputStream}
import org.dbpedia.spotlight.util.{SimpleUtils, GraphUtils, GraphConfiguration}
import org.apache.commons.logging.LogFactory

/**
 * Created with IntelliJ IDEA.
 * User: hector
 * Date: 7/3/12
 * Time: 8:55 PM
 */

/**
 * A merged graph using a method similar to the M&W method to incorporate co-occurrences graph and occurrences graph to capture the Wikipedia entity relations.
 *
 * The M&W's method on finding semantic relation between two wiki page is described here:
 * - D. Milne and I. H. Witten, “Learning to link with Wikipedia,” Proceeding of the 17th ACM conference on Information and knowledge mining - CIKM  ’08, p. 509, 2008.
 *
 * Here we are interested in the transpose graph of the occurrences graph because we are interested in the
 * in degree of a particular node -- which is the out  degree of the corresponding node in the transpose graph
 *
 * The two graph passed in should have the same number of nodes or a InitializationException will be thrown.
 * Actually, each index in these graph should be the same node, this was enforced during generation of graph
 * already using the HostMap.
 *
 * @param occTranGraph The transpose graph of the occurrences graph
 * @param cooccsGraph The co-occurrences graph
 */
class MWMergedGraph(occTranGraph: ArcLabelledImmutableGraph, cooccsGraph: ArcLabelledImmutableGraph) {
   private val LOG = LogFactory.getLog(this.getClass)
    val nodeNumber = occTranGraph.numNodes()

    if (occTranGraph.numNodes() != cooccsGraph.numNodes()){
      throw new InitializationException("The number of nodes of two graph are not the same")
    }

  /**
   * Get the in-link statistic for the occurrence graph, so actually the out links for the occurrences
   * transpose graph
   * @return A map from the source index to (out degree, target indices list) tuple
   */
    private def getInlinkStats(): Map[Int,(Int,Array[Int])]= {
      LOG.info("Counting indegree statistics")
      val iter = occTranGraph.nodeIterator()
      val map = new mutable.HashMap[Int,(Int,Array[Int])]()
      (1 to nodeNumber).foreach(count =>{
        val curr = iter.nextInt
        val d = iter.outdegree
        val succs = iter.successorArray().slice(0,d).sorted
        map += (curr -> (d,succs))
        if (count%500000 == 0){
          LOG.info(String.format("%s nodes visited.",count.toString))
        }
      })
      val res = map.toMap
      res
    }

  /**
   * Core function that build the integer list that represent the result graph
   * @param tripleListFile The output file store each arc per line as a tab separated triple
   */
    def buildTripleList(tripleListFile:File)  {
      val ilfo: OutputStream = new FileOutputStream(tripleListFile)
      val ilfoStream = new PrintStream(ilfo, true)

      val indegreeMap = getInlinkStats()
      val cooccsIter = cooccsGraph.nodeIterator()

      LOG.info("Building integer list for semantic graph, this will take a while")
      (1 to nodeNumber).foreach(count =>{
        val curr = cooccsIter.nextInt()

        val numCooccNodes = cooccsIter.outdegree
        val cooccSucc = cooccsIter.successorArray
        val cooccLabels = cooccsIter.labelArray

        (0 to numCooccNodes-1).foreach(idx => {
            val succ = cooccSucc(idx)
            val cooccCount = cooccLabels(idx).getInt
            val sr = mwSemanticRelatedness(indegreeMap(curr)._1,indegreeMap(succ)._1,indegreeMap(curr)._2,indegreeMap(succ)._2,cooccCount,nodeNumber)
            if (sr != 0.0){
              val str = curr + "\t" + succ + "\t" + sr
              ilfoStream.println(str)
            }
        })
        if (count%100000 == 0){
          LOG.info(String.format("%s nodes built.",count.toString))
        }
      })
      LOG.info("Done.")
    }

    // the return value is not necessarily Non-Negative. if maxIn/commonInLinks > wikisize/minIn, it will return negative value
    private def mwSemanticRelatedness(indegreeA:Int,indegreeB:Int,inlinksA:Array[Int],inlinksB:Array[Int],cooccCount:Int,wikiSize:Int) = {
      val commonInLinks = SimpleUtils.findCommonInSortedArray(inlinksA,inlinksB)
      val maxIn = math.max(indegreeA,indegreeB)
      val minIn = math.min(indegreeA,indegreeB)
      val r = 1-(math.log(maxIn)-math.log(commonInLinks))/(math.log(wikiSize) - math.log(minIn))
      r
//      if (r < 0){
//        LOG.warn(String.format("The computed semantic link is less than 0, indegreeA: %s, indegreeB: %s, commonInLinks: %s",indegreeA.toString,indegreeB.toString,commonInLinks.toString))
//      }
    }
}
