package org.dbpedia.spotlight.db.io

import edu.umass.cs.iesl.wikilink.expanded.WikiLinkItemIterator
import edu.umass.cs.iesl.wikilink.expanded.data.WikiLinkItem
import java.io.File

/**
 * Get iterator from wikilink file.
 *
 * @author Joachim Daiber
 */

object WikiLinkIterator {

  //get a iterator for WikiLinkItems
  def getIterator(wikiLinkFile: File): Iterator[WikiLinkItem] = {
    WikiLinkItemIterator.apply(wikiLinkFile)
  }

  def main(args: Array[String]) = {
    WikiLinkIterator.getIterator(new File(args(0))).foreach(item => {
      println(item.mentions)
    })
  }
}