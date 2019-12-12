/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package eoe.code

// FUTURE EHNANCEMENT! It'd be nifty to create a separate thread per file to be indexed up to, say, a maximum
// of 6 (configure in indexer.cfg?)
//
import groovy.json.JsonGenerator
import groovy.json.JsonOutput
import java.text.DecimalFormat
import java.text.SimpleDateFormat
import java.util.regex.Matcher
import java.util.regex.Pattern

import static java.lang.System.currentTimeMillis
import static java.lang.System.err

long start = currentTimeMillis()
if (args.length != 1) {
  Globals.usage()
}
Config cfg = new Config()
cfg.parseConfig(args[0])

// Walk the tree finding all files. If they match any of our patterns,
// deal with them.
new File(cfg.root).eachFileRecurse {
  String fName = it.getName()
  if (it.isFile() && fName.startsWith(".") == false &&
      (cfg.debug == false || Globals.firstBatchSent == false)
  ) {
    Matcher m = cfg.filePat.matcher(fName)
    if (m.find()) {
      Globals.whichIndexer.indexFile(it)
    }
  }
}
// Make sure to send anything in the doc list when we are done processing the
// last file
Globals.whichIndexer.sendBatch(true)
println("Finished run in (seconds)" + Globals.decimalFormat.format(((currentTimeMillis() - start) / 1000)) + " Total docs: " +
    Globals.decimalFormat.format(Globals.whichIndexer.docCount))

class Globals {
  static SolrSupportIndexerBase whichIndexer

  static usage() {
    err.println(("Usage: 'groovy SolrSupportIndexer.groovy path_to_config_file"))
    err.println("        path_to_config_file is mandatory, see the example config")
    err.println("        file for options and explanations, there's lots of help there.")
    System.exit(-1)
  }
  static DecimalFormat decimalFormat = new DecimalFormat("###,###,###")

  static firstBatchSent = false

  static def jsonOutput =
      new JsonGenerator.Options()
          .excludeNulls()  // Do not include fields with value null.
          .build()  // Create the converter instance.
}


abstract class SolrSupportIndexerBase {
  List<SolrDoc> docs = new ArrayList<>()
  File curFile
  Config cfg

  @Override
  String toString() {
    return super.toString()
  }

  // Send a batch of docs to Solr if there are enough docs in the list to
  // equal or exceed batch size. If force==true, send the docs in the list
  // no matter how few and commit at the end. force==true is intended to be
  // the very last call after processing _all_ files.
  void sendBatch(boolean force) {
    if ((force == false && docs.size() < cfg.batchSize) || docs.size() == 0) {
      return
    }
    Globals.firstBatchSent = true
    if (cfg.debug) {
      def json = Globals.jsonOutput.toJson(docs)
      println JsonOutput.prettyPrint(json)
    } else {
      def message = Globals.jsonOutput.toJson(docs)
      actuallySend(message, "application/json");
    }
    docs.clear()
    if (force) { // Yeah, yeah, yeah. This is clumsy to open a connection twice. So sue me
      actuallySend("commit=true", null)
      err.println "Sending commit after last file processed. Total documents sent: " + Globals.decimalFormat.format(docCount)
    }
  }

  void actuallySend(def message, String type) {
    def post = new URL(cfg.url).openConnection();
    post.setRequestMethod("POST")

    post.setDoOutput(true)
    if (type != null) {
      post.setRequestProperty("Content-Type", type)
    }

    post.getOutputStream().write(message.getBytes("UTF-8"));
    def postRC = post.getResponseCode();
    if (postRC != 200) {
      err.println("Got " + postRC + " back from server")
      Globals.usage()
    }
  }
  int docCount = 0;

  void addDoc(SolrDoc doc) {
    // A little awkward, but it keeps the field from being added to the doc.
    if (doc.tags_ss.size() == 0) {
      doc.tags_ss = null
    }
    if (doc.trace_txt.size() == 0) {
      doc.trace_txt = null
    }
    docs.add(doc);
    docCount++
    if ((docCount % 10_000) == 0) {
      err.println "Documents sent so far: " + Globals.decimalFormat.format(docCount)
    }

    if (docs.size >= cfg.batchSize) {
      sendBatch(false)
    }
  }

  abstract void indexFile(File file)

  String getBatch() {
    return cfg.batchTag ?: curFile.getName() + "_" + curFile.getAbsolutePath()
  }

  String decodeMe(String txt) {
    try {
      return URLDecoder.decode(txt, "UTF-8")
    } catch (Exception e) {
      return txt
    }
  }
  List<String> decodeMe(List<String> lines) {
    List<String> ret = new ArrayList<>();
    lines.each { line ->
      ret.add(decodeMe(line))
    }
  }
}

class SolrLogIndexer extends SolrSupportIndexerBase {
  @Override
  void indexFile(File file) {
    hitEOL = false
    curFile = file
    err.println "Parsing file: " + file.getAbsolutePath()
    file.withReader('UTF-8') {
      parseFile(it)
    }
  }

  enum LINE_TYPE {
    EXCEPTION,
    LOGLINE,
    EOF
  }

  void parseFile(BufferedReader br) throws IOException {
    List<String> lines = new ArrayList<>()
    while (true) {
      lines.clear()
      switch (getNextThingy(lines, br)) {
        case LINE_TYPE.EXCEPTION:
          addException(lines)
          break
        case LINE_TYPE.LOGLINE:
          addLogRecord(lines)
          break
        case LINE_TYPE.EOF:
          return
        default:
          errOut "Unrecognized return from getNextThingy: " + lines.get(0)
          break
      }
    }
  }

  Pattern digPat = Pattern.compile(".*?(\\d{1,4})")
  SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  // Get the date. Since the date formats are so strange let's do this manually.
  String getTime(String line) {
    for (Pattern pat : cfg.timePat) {
      Matcher m = pat.matcher(line)
      if (m.find()) {
        // Ugly. Really ugly. But what the heck..
        String toParse = m.group(1)
        m = digPat.matcher(toParse);
        if (m.find() == false) {
          err.println("Could not parse date: " + toParse)
          return;
        }
        int year = Integer.parseInt(m.group(1))
        toParse = toParse.substring(m.end(1))
        m = digPat.matcher(toParse)
        if (m.find() == false) {
          err.println("Could not parse date: " + toParse)
          return;
        }
        int month = Integer.parseInt(m.group(1))
        toParse = toParse.substring(m.end(1))

        m = digPat.matcher(toParse)
        if (m.find() == false) {
          err.println("Could not parse date: " + toParse)
          return;
        }
        int day = Integer.parseInt(m.group(1))
        toParse = toParse.substring(m.end(1))

        m = digPat.matcher(toParse)
        if (m.find() == false) {
          err.println("Could not parse date: " + toParse)
          return;
        }
        int hour = Integer.parseInt(m.group(1))
        toParse = toParse.substring(m.end(1))

        m = digPat.matcher(toParse)
        if (m.find() == false) {
          err.println("Could not parse date: " + toParse)
          return;
        }
        int minute = Integer.parseInt(m.group(1))
        toParse = toParse.substring(m.end(1))

        m = digPat.matcher(toParse)
        if (m.find() == false) {
          err.println("Could not parse date: " + toParse)
          return;
        }
        int second = Integer.parseInt(m.group(1))
        toParse = toParse.substring(m.end(1))

        m = digPat.matcher(toParse)
        int ms = 0
        if (m.find()) {
          ms = Integer.parseInt(m.group(1))
        }
        Calendar cal = new GregorianCalendar(year, month, day, hour, minute, second, ms);

        return sdf.format(cal.getTime())
      }
    }
    return null
  }

  void addException(List<String> lines) throws IOException {
    SolrDoc doc = new SolrDoc(getBatch(), curFile.getName(), docCount)
    doc.time_dt = getTime(lines.get(0))
    doc.tags_ss.add("EXCEPTION")
    doc.trace_txt.addAll(decodeMe(lines))
    Matcher m
    for (String line : lines) {
      // Don't bucket on the exception line since it can contain org.apache. etc. We want to bucket
      // on the pattern we've specified.
      boolean matched = false
      for (Pattern pat : cfg.exceptionPat) {
        m = pat.matcher(line)
        if (m.find()) {
          matched = true
          break
        }
      }
      if (matched) {
        continue
      }
      for (Pattern pat : cfg.threadStop) {
        m = pat.matcher(line)
        if (doc.bucket_s == null && m.find()) { // bucket at this line
          doc.bucket_s = line.trim()
          break
        }
      }
      for (Pattern pat : cfg.tagsToFind) {
        m = pat.matcher(line)
        if (m.find()) {
          doc.tags_ss.add(line.replaceAll("\"", "'"))
        }
      }
    }
    for (Pattern pat : cfg.collectionPat) {
      m = pat.matcher(lines.get(0))
      if (m.find()) {
        doc.collection_s = m.group(1)
        break;
      }
    }
    for (Pattern pat : cfg.corePat) {
      m = pat.matcher(lines.get(0))
      if (m.find()) {
        doc.core_s = m.group(1)
        break;
      }
    }

    addDoc(doc)
  }
  // This just outputs the line as a document, the point is to be able to sort on QTime, and/or facet on it etc.
  // There will only be a single line in the document

  void addLogRecord(List<String> lines) {
    SolrDoc doc = new SolrDoc(getBatch(), curFile.getName(), docCount)
    doc.time_dt = getTime(lines.get(0))
    int pos = lines.get(0).indexOf("params=")
    if (pos < 0) {
      doc.trace_txt.addAll(decodeMe(lines))
    } else {
      doc.trace_txt.add(decodeMe(lines.get(0).substring(0, pos)))
      String[] parts = lines.get(0).substring(pos).split("&")
      for (String part : parts) {
        if (part.trim().length() > 0) {
          doc.trace_txt.add(decodeMe(part))
        }
      }
    }
    for (String level : cfg.levels) {
      if (lines.get(0).contains(level)) {
        doc.tags_ss.add(level.trim().replaceAll("\"", "'"))
      }
    }
    Matcher m = cfg.qtimePat.matcher(lines.get(0))
    if (m.find()) {
      doc.qtime_i = Integer.parseInt(m.group(1))
    }

    m = cfg.handlerPat.matcher(lines.get(0))
    if (m.find()) {
      doc.handler_s = m.group(1)
    }
    m = cfg.rowsPat.matcher(lines.get(0))
    if (m.find()) {
      doc.rows_i = Integer.parseInt(m.group(1))
    }
    m = cfg.startPat.matcher(lines.get(0))
    if (m.find()) {
      doc.start_i = Integer.parseInt(m.group(1))
    }
    m = cfg.slowPat.matcher(lines.get(0))
    if (m.find()) {
      doc.tags_ss.add("SLOW")
    }
    m = cfg.corePat.matcher(lines.get(0))
    if (m.find()) {
      if (m.group(1).endsWith(",")) { // hacky!
        doc.core_s = m.group(1).substring(0, m.group(1).length() - 1)
      } else {
        doc.core_s = m.group(1)
      }
    }
    for (Pattern pat : cfg.collectionPat) {
      m = pat.matcher(lines.get(0))
      if (m.find()) {
        doc.collection_s = m.group(1)
        break;
      }
    }
    addDoc(doc)
  }

  boolean hitEOL = false
  String lastLine = null

  // Get the next coherent entity. If it's an exception, the entire exception will be returned as a list of strings.
  // If a "plain" log line, then the list will have only one entry.
  LINE_TYPE getNextThingy(List<String> thingy, BufferedReader br) throws IOException {
    String line = lastLine
    if (line == null) {
      line = br.readLine()
      if (line != null) {
        line = line.trim()
      }
    } else {
      line = lastLine
    }
    lastLine = null
    if (line == null) {
      return LINE_TYPE.EOF
    }
    for (Pattern pat : cfg.exceptionPat) {
      Matcher m = pat.matcher(line)
      if (m.find()) {
        thingy.add(line)
        getException(thingy, br)
        return LINE_TYPE.EXCEPTION
      }
    }
    thingy.add(line.trim())
    return LINE_TYPE.LOGLINE
  }

  void getException(List<String> oneException, BufferedReader br) throws IOException {
    if (hitEOL) {
      return
    }
    String line
    while ((line = br.readLine()) != null) {
      for (Pattern pat : cfg.timePat) {
        Matcher m = pat.matcher(line)
        if (m.find()) {
          lastLine = line
          return
        }
      }
      oneException.add(line.trim())
    }
    if (line == null) {
      hitEOL = true
    }
    return
  }
}

class SolrThreadIndexer extends SolrSupportIndexerBase {

  @Override
  void indexFile(File file) {
    curFile = file
    file.withReader('UTF-8') {
      parseThreadDumpFile(it)
    }
  }
  boolean hitEOL = false
  String lastLine = null

  void parseThreadDumpFile(BufferedReader br) throws IOException {
    List<String> oneThread = new ArrayList<>()
    hitEOL = false
    // First get past the cruft at the beginning:
    String line
    while ((line = br.readLine()) != null) {
      if (line.trim().length() == 0) break
    }
    while (true) {
      oneThread.clear()
      String state = advanceToNextState(oneThread, br)
      if (state == null) {
        if (hitEOL) {
          return // We're done.
        }
        continue
      }
      outputThread(state, oneThread)
    }
  }

  String advanceToNextState(List<String> oneThread, BufferedReader br) throws IOException {
    if (hitEOL) {
      return null
    }
    String state

    String line
    boolean inTrace = false
    while (true) {
      if (lastLine != null) {
        line = lastLine
        lastLine = null
      } else {
        line = br.readLine()
      }
      if (line == null) {
        hitEOL = true
        return state
      } else {
        if (line.trim().length() == 0) {
          continue
        }
        if (Character.isWhitespace(line.charAt(0)) == false && inTrace) {
          lastLine = line
          break
        }
        inTrace = true
        oneThread.add(line)
        Matcher m = cfg.statePat.matcher(line)
        if (m.find()) {
          state = m.group(1)
        }
      }
    }
    return state
  }

  void outputThread(String state, List<String> thread) throws IOException {
    SolrDoc doc = new SolrDoc(getBatch(), curFile.getName(), docCount);
    doc.batch_s = getBatch();
    // Add any regex patterns specified in addition to any wait objects
    doc.trace_txt.addAll(thread)
    doc.state_s = state
    Matcher m;
    for (String line : thread) {
      if (doc.bucket_s == null) {
        for (Pattern pat : cfg.threadStop) {
          m = pat.matcher(line)
          if (m.find()) { // bucket at this line
            doc.bucket_s = line.trim()
            break
          }
        }
      }
      for (Pattern pat : cfg.tagsToFind) {
        m = pat.matcher(line)
        if (m.find()) {
          doc.tags_ss.add(m.group(1) + "_" + pat.toString().replaceAll("\"", "'"))
        }
      }
    }
    addDoc(doc)
  }
}
// Just a serializable object to make writing JSON much easier. The
// int values are defs so the nifty "do not print if null" bits of
// serializing this object to JSON work without any extra effort.
// Occasionally the non-typed stuff is helpful )
class SolrDoc {
  String id
  String bucket_s
  List<String> tags_ss = new ArrayList<>()
  List<String> trace_txt = new ArrayList<>()
  def rows_i
  def start_i
  def qtime_i
  String handler_s
  String core_s
  String state_s
  String time_dt
  String collection_s
  String file_s
  String batch_s

  SolrDoc(String batch, String file, int docCount) {
    this.id = batch + "_" + docCount
    this.file_s = file
    this.batch_s = batch
  }
}
/**
 * Holds the parsed configuration, which got quite large eventually!
 */
class Config {
  private String batchTag
  private List<Pattern> timePat = new ArrayList<>()
  private List<Pattern> exceptionPat = new ArrayList<>()
  private Pattern handlerPat
  private String root
  private List<Pattern> tagsToFind = new ArrayList<>()
  private List<Pattern> threadStop = new ArrayList<>()
  private Pattern slowPat
  private List<String> levels = new ArrayList<>()
  private Pattern corePat
  private Pattern filePat = Pattern.compile(".*")
  private Pattern statePat
  private Pattern rowsPat
  private Pattern startPat
  private String url
  private boolean debug = false
  private int batchSize = 1000
  private Pattern qtimePat
  private List<Pattern> collectionPat = new ArrayList<>()

  void parseConfig(String file) throws IOException {
    new File(file).eachLine { line ->
      int pos = line.indexOf('#')
      if (pos >= 0) {
        line = line.substring(0, pos).trim()
      }
      if (line.trim().length() == 0) {
        return
      }
      pos = line.indexOf("=")
      if (pos < 0) {
        err.println("malformed line in config file: " + line)
        Globals.usage()
      }
      String[] parts = new String[2]
      parts[0] = line.substring(0, pos).trim()
      parts[1] = line.substring(pos + 1).trim()
      switch (parts[0].toLowerCase()) {
        case "batch":
          batchTag = parts[1]
          break
        case "tagspat":
          tagsToFind.add(Pattern.compile(parts[1]))
          break
        case "slowpat":
          slowPat = Pattern.compile(parts[1])
          break
        case "corepat":
          corePat = Pattern.compile(parts[1])
          break
        case "levels":
          String[] levelParts = parts[1].split("[^a-zA-Z0-9]")
          for (String lev : levelParts) {
            if (lev.trim().length() == 0) {
              continue
            }
            levels.add(lev.trim())
          }
          break
        case "timepat":
          timePat.add(Pattern.compile(parts[1]))
          break
        case "exceptionpat":
          exceptionPat.add(Pattern.compile(parts[1]))
          break
        case "handlerpat":
          handlerPat = Pattern.compile(parts[1])
          break
        case "filepat":
          filePat = Pattern.compile(parts[1])
          break
        case "path":
          root = parts[1]
          break
        case "threadstoppat":
          threadStop.add(Pattern.compile(parts[1]))
          break
        case "batchsize":
          batchSize = Integer.parseInt(parts[1])
          break
        case "filetype":
          if ("logfile".equals(parts[1].toLowerCase())) {
            Globals.whichIndexer = new SolrLogIndexer()
          } else if ("threaddump".equals(parts[1].toLowerCase())) {
            Globals.whichIndexer = new SolrThreadIndexer()
          } else {
            err.println("Unrecognied value for 'fileType' parameter in the configuration file: " + parts[1])
            Globals.usage()
          }
          break
        case "statepat":
          statePat = Pattern.compile(parts[1])
          break
        case "rowspat":
          rowsPat = Pattern.compile(parts[1])
          break
        case "startpat":
          startPat = Pattern.compile(parts[1])
          break
        case "url":
          url = parts[1]
          if ("STDOUT".equals(url) == false && url.endsWith("update") == false) {
            if (url.endsWith("/") == false) {
              url += "/"
            }
            url += "update"
          }
          break
        case "debug":
          debug = Boolean.parseBoolean(parts[1])
          break
        case "qtimepat":
          qtimePat = Pattern.compile(parts[1])
          break
        case "collectionpat":
          collectionPat.add(Pattern.compile(parts[1]))
          break;
        default:
          err.println("Unrecognized line in config file: " + line)
          Globals.usage()
          break
      }
    }
    if (Globals.whichIndexer == null) {
      err.println("You must specify a file type in the configuration file")
      Globals.usage()
    }
    Globals.whichIndexer.cfg = this
    echoParams()
  }

  @Override
  String toString() {
    return super.toString()
  }

  void echoParams() {
    err.println("Running with params: ")
    err.println("    debug: " + debug.toString())
    err.println("    url: " + url)
    err.println("    root: " + root)
    err.println("    filepat: " + filePat.toString())
    err.println("    batch: " + (batchTag ?: " fully qualified path for each file processed"))
    err.println("    batchSize: " + Globals.decimalFormat.format(batchSize))

    for (Pattern pat : tagsToFind) {
      err.println("    tagsPat: " + pat.toString())
    }
    for (Pattern pat : tagsToFind) {
      err.println("    Tag: " + pat.toString())
    }
    err.println("    slowPat: " + slowPat.toString())
    err.println("    corePat: " + corePat.toString())
    for (String lev : levels) {
      err.println("    Level: " + lev)
    }

    for (Pattern pat : exceptionPat) {
      err.println("    ExceptionPat: " + pat.toString())
    }
    err.println("    statePat: " + statePat.toString())
    for (Pattern pat : timePat) {
      err.println("    timePat: " + pat.toString())
    }
    err.println("    handlerPat: " + handlerPat.toString())
    for (Pattern pat : threadStop) {
      err.println("    threadStop: " + pat.toString())
    }
    err.println("    rowsPat: " + rowsPat.toString())
    err.println("    startPat: " + startPat.toString())
  }
}
