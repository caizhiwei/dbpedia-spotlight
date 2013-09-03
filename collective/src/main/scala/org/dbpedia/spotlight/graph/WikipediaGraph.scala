package org.dbpedia.spotlight.graph

import io.Source
import org.apache.commons.logging.LogFactory
import org.dbpedia.spotlight.model.DBpediaResource
import java.io.{PrintStream, FileOutputStream, OutputStream, File}
import org.dbpedia.spotlight.util.{GraphUtils, GraphConfiguration, SimpleUtils}
import org.json.JSONObject
import sun.util.calendar.LocalGregorianCalendar
import org.json
import com.officedepot.cdap2.collection.CompactHashMap

/**
 *
 * Store a graph from pignlproc output data
 *
 * @author hector.liu
 * @author zhiwei
 */

/**
 * Create a Integer list to help build a occurrence and co-occurrence graph (use GraphUtils$ to build)
 * This class will use the Uri-Integer map (Host Map) so that it make sure the same index
 * in both graph will point to the same url.
 *
 * It could parse both JSON files or PigStorage files.
 *
 * You can run this class independently. But the generation of this graph has already been included in GraphMaker.
 * So if you have run that class successfully, you don't have to do so.
 */
object WikipediaGraph {
  private val LOG = LogFactory.getLog(this.getClass)

  /**
   * Parse the count file generated using PigStorage
   * @param fileList The list of count file stored using PigStorage
   * @param hostMap The hostMap from uri to index
   * @param ilfoStream Integer List file output stream
   */
  private def parsePigStorageFile(fileList:List[File],hostMap:CompactHashMap[String,Int],ilfoStream:PrintStream){
    var count = 0

    fileList.foreach(f =>Source.fromFile(f).getLines().filterNot(line => line.trim() == "").foreach(
      line => {
        val fields = line.split("\\t")

        if (fields.length == 2){
          val srcUri = fields(0)
          val srcIdx = hostMap.getOrElse(srcUri,-1)
          if (srcIdx == -1){
            LOG.error(String.format("Uri [%s] was not found in host map, if this happens a lot, something might be wrong",srcUri))
          }else{
            val targetMap = SimpleUtils.hadoopTuplesToMap(fields(1))
            targetMap.foreach{
              case(tarUri,cooccCount) => {
                val tarIdx = hostMap.getOrElse(tarUri,-1)
                if (tarIdx == -1){
                  LOG.error(String.format("Uri [%s] was not found in host map, if this happens a lot, something might be wrong",tarUri))
                }else{
                  if (srcIdx != tarIdx){
                    //co-occurrences are bi-directional, but only save one direction may save space
                    val intString = srcIdx + "\t" + tarIdx + "\t" + cooccCount
                    ilfoStream.println(intString)
                  }else{
                    //LOG.warn("We don't particularly welcome self co-occurrences.")
                  }
                }
              }
            }
          }
        }else  LOG.error("Invailid line in file at \n -> \t" + line)
        count += 1
        if (count % 100000 == 0) LOG.info(String.format("%s lines processed.",count.toString))
      }))
  }

  /**
   * Parse the cooccsFile generated using JSONStorage, it could also directly parse the Pig generated directory
   * @param fileList The file list of files that stores cooccurrences count using JSONStorage
   * @param hostMap The host map that map from uri to index
   * @param ilfoStream  Integer List file output stream
   */
  private def parseJSONStorageFile(fileList:List[File],hostMap:CompactHashMap[String,Int],ilfoStream:PrintStream) {
    var count = 0

    fileList.foreach(f => Source.fromFile(f).getLines().filterNot(l => l.trim() == "").foreach(
      line =>{
        val js = new json.JSONObject(line)

        val srcUri = js.getString("src")
        val srcIdx = hostMap.getOrElse(srcUri,-1)

        if (srcIdx == -1)
          LOG.error(String.format("Uri [%s] was not found in host map, if this happens a lot, something might be wrong",srcUri))
        else{
          val countArray = js.getJSONArray("counts")
          val len = countArray.length()
          (0 to len -1).foreach(i => {
            val countPair = countArray.getJSONObject(i)
            if (countPair != null){

              val tarUri = countPair.getString("tar")
              val count = countPair.getString("count")
              val tarIdx = hostMap.getOrElse(tarUri,-1)

              if (tarIdx == -1){
                LOG.error(String.format("Uri [%s] was not found in host map, if this happens a lot, something might be wrong",tarUri))
              }else{
                if (srcIdx != tarIdx){
                  //co-occurrences are bi-directional, but only save one direction may save space
                  val intString = srcIdx + "\t" + tarIdx + "\t" + count
                  ilfoStream.println(intString)
                }else{
                  //LOG.warn("We don't particularly welcome self co-occurrences.")
                }
              }

            }
          })
        }

        count += 1
        if (count % 100000 == 0) LOG.info(String.format("%s objects processed.",count.toString))
      }
    ))
  }

  /**
   * Parse the coocurrence count file(s) into cooccurence integer list given a hostMap.
   * @param cooccsFile the cooccurrences count file(s), could be a directory containing the raw Pig output, or a merged file
   * @param hostMap  The host map from uri to integer
   * @param tripleListFile  The output file store each arc per line as a tab separated triple
   */
  def buildTripleList(cooccsFile:File , hostMap:CompactHashMap[String,Int] , tripleListFile:File) {
    LOG.info("Parsing Cooccurrences into Integer List")

    val ilfo: OutputStream = new FileOutputStream(tripleListFile)
    val ilfoStream = new PrintStream(ilfo, true)

    val fileList =
      if (cooccsFile.isDirectory){
        cooccsFile.listFiles().filter(f=> SimpleUtils.isPigPartFile(f)).sorted.toList
      }  else{
        List(cooccsFile)
      }

    if (cooccsFile.getName.endsWith(".tsv")){
      parsePigStorageFile(fileList,hostMap,ilfoStream)
    }else{
      parseJSONStorageFile(fileList,hostMap,ilfoStream)
    }
  }
}