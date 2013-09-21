package org.dbpedia.spotlight.graph

import java.io.{PrintWriter, File}
import es.yrbcn.graph.weighted.WeightedBVGraph
import org.dbpedia.spotlight.util.{SimpleUtils, GraphConfiguration, GraphUtils}
import org.apache.commons.logging.LogFactory
import scala.io.Source
import com.officedepot.cdap2.collection.CompactHashMap
import it.unimi.dsi.webgraph.labelling.ArcLabelledImmutableGraph

/**
 *
 * Store a graph from pignlproc output data
 *
 * @author hector.liu
 * @author zhiwei
 */

/**
 * This class is used during indexing to create the graphs needed for disambiguation.
 *
 * It simply call different of graph classes to generate different Wikipedia link graph. The graph classes
 * simply return arc list to represent the graph, this class call WebGraph methods to store them in WebGraph format.
 *
 * If new kinds of graphs are implemented, they can be also added here so that they constructed at one run.
 *
 * Please note that all graphs should be passed with a numberOfNodes parameter to ensure they are of the same
 * size, and the index will conform to which recorded in the HostMap
 */
object GraphMaker{
  private val LOG = LogFactory.getLog(this.getClass)

  def makeGraph(hostMap: CompactHashMap[String,Int], numberOfNodes:Int, srcFilePath: String,
                listFilePath: String, graphFilePath: String) : ArcLabelledImmutableGraph = {
    //Generating for co-occurrences files
    val srcFile = new File(srcFilePath)
    val listFile = new File(listFilePath)
    //parse the cooccsSrcFile and store the parsed result as an IntegerList
    WikipediaGraph.buildTripleList(srcFile,hostMap,listFile)

    //build a weighted graph and store.
    //We should use the method that specify a node number, which make it possible to have nodes with no arcs
    GraphUtils.buildWeightedGraphFromFile(listFile,numberOfNodes)
  }
  /**
   * Create and store the occurrence graph
   * @param baseDir Base dir for graph storage
   * @param config Graph config
   * @param hostMap The host map parsed from the uriMap file
   * @param numberOfNodes The number of nodes stored in HostMap
   */
  def makeCooccGraph(baseDir: String, config:GraphConfiguration, hostMap: CompactHashMap[String,Int], numberOfNodes:Int){
    val cooccSubDir = baseDir+config.get("org.dbpedia.spotlight.graph.coocc.dir")
    SimpleUtils.createDir(cooccSubDir)

    val cooccsSrcFilePath = config.get("org.dbpedia.spotlight.graph.coocc.src")
    val cooccInterListFilePath = cooccSubDir+config.get("org.dbpedia.spotlight.graph.coocc.integerList")
    val cooccGraphPath = cooccSubDir+config.get("org.dbpedia.spotlight.graph.coocc.basename")
    val graph = makeGraph(hostMap, numberOfNodes, cooccsSrcFilePath, cooccInterListFilePath, cooccGraphPath)
    GraphUtils.storeWeightedGraph(graph,cooccGraphPath)
  }
  /**
   * Create and store the co-occurrence graph
   * @param baseDir Base dir for graph storage
   * @param config Graph config
   * @param hostMap The host map parsed from the uriMap file
   * @param numberOfNodes The number of nodes stored in HostMap
   */
  def makeOccsGraph(baseDir: String, config:GraphConfiguration, hostMap: CompactHashMap[String,Int], numberOfNodes:Int){
    val occSubDir = baseDir+config.get("org.dbpedia.spotlight.graph.occ.dir")
    SimpleUtils.createDir(occSubDir)

    val occsSrcFilePath = config.get("org.dbpedia.spotlight.graph.occ.src")
    val occInterListFilePath = occSubDir+config.get("org.dbpedia.spotlight.graph.occ.integerList")
    val occGraphPath = occSubDir+config.get("org.dbpedia.spotlight.graph.occ.basename")
    val graph = makeGraph(hostMap, numberOfNodes, occsSrcFilePath, occInterListFilePath, occGraphPath)
    GraphUtils.storeWeightedGraph(graph,occGraphPath)

    val occTransposeBaseName = config.get("org.dbpedia.spotlight.graph.transpose.occ.basename")
    val batchSize = config.get("org.dbpedia.spotlight.graph.transpose.batchSize").toInt

    //also store the transpose graph, easy to use outdegree to find the indegree of a node in the origin graph
    val ocwgTrans = GraphUtils.transpose(graph,batchSize)
    GraphUtils.storeWeightedGraph(ocwgTrans,occSubDir+occTransposeBaseName)
  }
  def makeMWMergedGraph(graphConfig: GraphConfiguration){
    val offline = "true" == graphConfig.getOrElse("org.dbpedia.spotlight.graph.offline","false")

    LOG.info("Preparing graphs...")

    val baseDir = graphConfig.get("org.dbpedia.spotlight.graph.dir")
    val cooccSubDir = baseDir+graphConfig.get("org.dbpedia.spotlight.graph.coocc.dir")
    val cooccGraphBasename = graphConfig.get("org.dbpedia.spotlight.graph.coocc.basename")

    val occTransposeSubDir = baseDir+graphConfig.get("org.dbpedia.spotlight.graph.occ.dir")
    val occTransposeGraphBaseName = graphConfig.get("org.dbpedia.spotlight.graph.transpose.occ.basename")

    val rowg = GraphUtils.loadAsArcLablelled(occTransposeSubDir,occTransposeGraphBaseName, offline)
    val cwg = GraphUtils.loadAsArcLablelled(cooccSubDir,cooccGraphBasename,offline)


    //preparing output graph
    val sgSubDir = baseDir+graphConfig.get("org.dbpedia.spotlight.graph.semantic.dir")
    SimpleUtils.createDir(sgSubDir)
    val sgBasename = graphConfig.get("org.dbpedia.spotlight.graph.semantic.basename")
    val sgIntegerListFileName = sgSubDir+graphConfig.get("org.dbpedia.spotlight.graph.semantic.integerList")

    val sg = new MWMergedGraph(rowg,cwg)

    val sgFile = new File(sgIntegerListFileName)

    sg.buildTripleList(sgFile)

    val g = GraphUtils.buildWeightedGraphFromFile(sgFile,graphConfig.getNodeNumber)

    GraphUtils.storeWeightedGraph(g,sgSubDir+sgBasename)
  }
  //parameter is the graph properties file: ../../conf/graph.properties
  def main(args: Array[String]) {
    val graphConfigFileName = args(0)
    val config = new GraphConfiguration(graphConfigFileName, false)

    val baseDir = config.get("org.dbpedia.spotlight.graph.dir")

    //create if not exists
    SimpleUtils.createDir(baseDir)

    val uriMapFile = new File(config.get("org.dbpedia.spotlight.graph.dir")+config.get("org.dbpedia.spotlight.graph.mapFile"))
    if(!uriMapFile.exists()) {
      uriMapFile.createNewFile();
    }
    val occsTempFile = new File(config.get("org.dbpedia.spotlight.graph.occ.temp"))

    //Generate the host map
    val numberOfNodes = HostMap.parseToHostMap(occsTempFile,uriMapFile)
    val outWriter = new PrintWriter(new File(config.get("org.dbpedia.spotlight.graph.dir")+"/nodenumber"))
    outWriter.println(numberOfNodes)
    outWriter.close()

    //Get the host map
    val hostMap = HostMap.load(uriMapFile)

    makeOccsGraph(baseDir,config,hostMap,numberOfNodes)
    makeCooccGraph(baseDir,config,hostMap,numberOfNodes)
    makeMWMergedGraph(config)
  }
}