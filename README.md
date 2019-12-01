# SolrSupportIndexer
Groovy script to index Solr logfiles and thread dumps and view them with the /browse handler

This is a WIP, I'd _really_ like people to give it a spin. And contact Erick Erickson if it doesn't work. Or if you want to add more functionality. Or just fix it...

**To try it:**

1. start up a Solr instance with the attached configs. It really doesn't matter whether it's SolrCloud or stand-alone.

2. Edit the sample "indexer.cfg" file. The first 70 lines or so are what needs to be edited most often. *Don't be alarmed!!*, it's only 6 actual entries and a lot of comments.

3. Execute "groovy SolrSuppportIndexer path_to_config_file_from_step_2". You should see progress reports almost immediately.

4. Point your browser to your_solr_instance:port/solr/your_collection_or_core_name/browse after step <3> is complete.

5. Kick the tires as much as you want. I was going to include sample log and thread dumps, but there's too much chance of there being proprietary info in them, even just the collection names. So be brave. Try it on one of your favorite log files or thread dumps. I'll be happy to help tweak the cfg file for your particular issue.

6. Seriously think about how we can improve it. In particular, the UI is the browse handler, which is _probably_ going away soon from Solr. Hey! There's this nifty Fusion product that could provide a _much_ better front-end etc. Volunteers?

My testing was done with Solr 8.2. There are no a-priori reasons it wouldn't work with earlier versions, but that's not tested.

## How it works
* You specify a root path and file pattern in the configuration file. The program recursively traverses the filesystem, starting at the root and indexes each file that matches the file pattern.
* You have to specify whether it's looking at a logfile or a thread dump. Maybe we can make this automatic if we use this approach regularly.
* Log Files:
  * For each "regular" log line, it extracts selected bits of information, qtime, replica, etc. and puts them in facetable fields in an individual document. 
  * For each exception it indexes the entire exception in a single document, again extracting "interesting" tidbits for faceting etc. The UI will have a facet on log level messages, slow queries etc.
* Thread Dumps:
  * Each stack is indexed as a single document, with various "interesting" bits of information put in separate fields.
  * Each thread dump _also_ adds a tag for the first regex it encounters to "bucket" on. This means that you'll have a facet available for, say, the first line in the stack trace that mentions "org.apache.solr (rest of line spec here)". Effectively, you'll have an available count of all the threads that are stalled on the same line of Lucene/Solr code.
    * If you specify multiple patterns (see the config file, threadStopPat entries), each stack trace will be bucketed on the first one of the regexes it finds in the order defined in the config file.
* Tweak the configuration file if it's not handlng a specific file properly. I'll be glad to work with anyone who's having difficulties.

## Weaknesses

* The UI is messy. There are too many facets. Facets are hard-coded, to change them you need to change the Velocity templates. Text bleeds over into other text (hover over the facets to see the whole thing). Etc. Try to overlook that part and see if the funcitonality is useful. If so, we can move it all to a better UI.
  * Some of the documents are sparse. Look at some of the facets to see documents with more attributes.
* The configuration file may take tweaking, and you have to deal with Java regexes in order to do so. There are extensive comments in the configuration file, feel free to add more. I decided to go with regexes since there are too many possibilities. I hope we can accumulate enough that the need to add more will taper off. Here are two examples:
  * One set of log files had a timestamp with a 'T' between the date and time. Another one had a space. Siiigggh. So I put in two patterns. It's a balance between multiple entries and complex entries, there's no particuarly correct answer.
  * When we get zip files, each line can start with the name of the log file, e.g. "solr.log.1:2019-11-10T00:12:34"
* I took a stab at extracting and faceting data for sanity checks, for instance there's a "rows_i" and a "start_i" field that has facets in the UI. This is by no means complete, but we can add as many more as we want. Or remove useless ones.
  * It'd be nice for a sophisticated UI able to turn thes on and off, there are quite a number of them. Hint. Hint. Hint.
* It's yet another "roll you own" solution. This is specialized for Solr logs though, so it may be worth the maintenance.

## Enhancements
There are about a zillion things that might be done here. If it has enough functionality in its current form, we can put in the effort. I wanted to get something running quickly to see if it has possibilities. Then we might make it maximally useful. Some ideas:

* Make it a hosted Fusion site.
  * Allow us to upload files to it and keep it permanently running. Drag/drop would be cool! Or a datasource. Or...
  * Stop using an external indexing program. Take what the current one does and make Fusion do its tricks.
  * Allow customers to use it and/or embed something similar in Fusion.
* Speed it up. Currently it's single threaded for instance. The heavy use of regexes can be slow. That said, so far it's fast enough for the samples I've ued so far.
* Embed it in Fusion in the hope that clients can run it themselves and/or we can do screen shares and resolve support cases faster.
* Have Fusion be able to go out and collect log files and do this automatically (well, on demand instead probably)
* Decide that Splunk or ES can already do this for us and abandon it.

Please let me know if you have any questions.
