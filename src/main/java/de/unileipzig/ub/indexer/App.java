package de.unileipzig.ub.indexer;

import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.deletebyquery.IndexDeleteByQueryResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.AndFilterBuilder;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.TermFilterBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
// import static org.elasticsearch.index.query.xcontent.FilterBuilders.*;
// import static org.elasticsearch.index.query.xcontent.QueryBuilders.*;

/**
 * Hello world!
 *
 * Copyright (C) 2012 Martin Czygan, martin.czygan@uni-leipzig.de Leipzig
 * University Library, Project finc http://www.ub.uni-leipzig.de
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 *
 * @author Martin Czygan
 * @license http://opensource.org/licenses/gpl-3.0.html GNU General Public
 * License
 * @link     http://finc.info
 *
 */
public class App {

    private static Logger logger = Logger.getLogger(App.class.getCanonicalName());
    private static MemcachedClient memcachedClient;
    private static ObjectMapper sharedMapper = new ObjectMapper();

    public static String extractSrcSHA1(String filename) throws IOException {
        // extract the meta.srcsha1 attribute from filename
        String srcSHA1 = null;
        try {
            final FileReader fr = new FileReader(filename);
            final BufferedReader br = new BufferedReader(fr);
            String line;
            // one line is one document
            while ((line = br.readLine()) != null) {
                final ObjectMapper mapper = new ObjectMapper();
                final HashMap record = mapper.readValue(line, HashMap.class);
                final HashMap meta = (HashMap) record.get("meta");
                srcSHA1 = meta.get("srcsha1").toString();
                break;
            }
        } catch (IOException ioe) {
            logger.error(filename + ": " + ioe);
            System.exit(1);
        }
        if (srcSHA1 == null) {
            logger.fatal(filename + ": meta.srcsha1 not found. Aborting.");
            throw new IOException("meta.srcsha1 not found. Aborting.");
        }
        return srcSHA1;
    }

    public static long getLineCount(String filename) throws FileNotFoundException, IOException {

        long lines = 0;
        final File file = new File(filename);
        if (file.length() == 0) {
            return 0L;
        }

        // use memcached if available
        final String mKey = "getLineCount::" + filename + "::" + file.lastModified();

        if (memcachedClient != null) {
            final Object cachedObject = memcachedClient.get(mKey);
            if (cachedObject != null) {
                try {
                    lines = Long.parseLong(cachedObject.toString());
                    logger.debug("cache hit on: " + mKey);
                    return lines;
                } catch (NumberFormatException e) {
                    // invalidate this cache item
                    memcachedClient.delete(mKey);
                }
            }
        }

        BufferedReader reader = new BufferedReader(new FileReader(filename));
        while (reader.readLine() != null) {
            lines++;
        }
        reader.close();

        if (memcachedClient != null) {
            memcachedClient.set(mKey, 2592000, lines);
            logger.debug("updated cache: " + mKey);
        }

        return lines;
    }

    public static long getIndexedRecordCount(Client client, String indexName, String sha1) {
        boolean indexExists = client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists();
        if (!indexExists) {
            return 0;
        }
        CountResponse response = client.prepareCount(indexName).setQuery(termQuery("meta.srcsha1", sha1)).execute().actionGet();
        return response.getCount();
    }

    public static String getContentIdentifier(String line, String identifier) throws IOException {
        final HashMap record = sharedMapper.readValue(line, HashMap.class);
        final HashMap content = (HashMap) record.get("content");
        final String id = content.get(identifier).toString();
        return id;
    }

    public static String getTimestamp(String line) throws IOException {
        final HashMap record = sharedMapper.readValue(line, HashMap.class);
        final HashMap meta = (HashMap) record.get("meta");
        final String timestamp = meta.get("timestamp").toString();
        return timestamp;
    }

    public static String setLatestFlag(String line) throws IOException {
        final HashMap record = sharedMapper.readValue(line, HashMap.class);
        record.put("latest", "true");
        return sharedMapper.writeValueAsString(record);
    }

    public static void quit(int code) {
        if (memcachedClient != null) {
            memcachedClient.shutdown();
        }
        System.exit(code);
    }

    public static void main(String[] args) throws IOException {

        // create Options object
        Options options = new Options();

        options.addOption("h", "help", false, "display this help");

        options.addOption("f", "filename", true, "name of the JSON file whose content should be indexed");
        options.addOption("i", "index", true, "the name of the target index");
        options.addOption("d", "doctype", true, "the name of the doctype (title, local, ...)");

        options.addOption("t", "host", true, "elasticsearch hostname (default: 0.0.0.0)");
        options.addOption("p", "port", true, "transport port (that's NOT the http port, default: 9300)");
        options.addOption("c", "cluster", true, "cluster name (default: elasticsearch_mdma)");

        options.addOption("b", "bulksize", true, "number of docs sent in one request (default: 3000)");
        options.addOption("v", "verbose", false, "show processing speed while indexing");
        options.addOption("s", "status", false, "only show status of index for file");

        options.addOption("r", "repair", false, "attempt to repair recoverable inconsistencies on the go");
        options.addOption("e", "debug", false, "set logging level to debug");
        options.addOption("l", "logfile", true, "logfile - in not specified only log to stdout");

        options.addOption("m", "memcached", true, "host and port of memcached (default: localhost:11211)");
        options.addOption("z", "latest-flag-on", true, "enable latest flag according to field (within content, e.g. 001)");
        options.addOption("a", "flat", false, "flat-mode: do not check for inconsistencies");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException ex) {
            logger.error(ex);
            System.exit(1);
        }

        // setup logging
        Properties systemProperties = System.getProperties();
        systemProperties.put("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.Log4JLogger");
        System.setProperties(systemProperties);
        Logger.getLogger("net.spy.memcached").setLevel(Level.ERROR);

        Properties props = new Properties();
        props.load(props.getClass().getResourceAsStream("/log4j.properties"));

        if (cmd.hasOption("debug")) {
            props.setProperty("log4j.logger.de.unileipzig", "DEBUG");
        }

        if (cmd.hasOption("logfile")) {
            props.setProperty("log4j.rootLogger", "INFO, stdout, F");
            props.setProperty("log4j.appender.F", "org.apache.log4j.FileAppender");
            props.setProperty("log4j.appender.F.File", cmd.getOptionValue("logfile"));
            props.setProperty("log4j.appender.F.layout", "org.apache.log4j.PatternLayout");
            props.setProperty("log4j.appender.F.layout.ConversionPattern", "%5p | %d | %F | %L | %m%n");
        }

        PropertyConfigurator.configure(props);


        InetAddress addr = InetAddress.getLocalHost();
        String memcachedHostAndPort = addr.getHostAddress() + ":11211";
        if (cmd.hasOption("m")) {
            memcachedHostAndPort = cmd.getOptionValue("m");
        }

        // setup caching
        try {
            if (memcachedClient == null) {
                memcachedClient = new MemcachedClient(
                        new ConnectionFactoryBuilder().setFailureMode(FailureMode.Cancel).build(),
                        AddrUtil.getAddresses("0.0.0.0:11211"));
                try {
                    // give client and server 500ms
                    Thread.sleep(300);
                } catch (InterruptedException ex) {
                }

                Collection availableServers = memcachedClient.getAvailableServers();
                logger.info(availableServers);
                if (availableServers.size() == 0) {
                    logger.info("no memcached servers found");
                    memcachedClient.shutdown();
                    memcachedClient = null;
                } else {
                    logger.info(availableServers.size() + " memcached server(s) detected, fine.");
                }
            }
        } catch (IOException ex) {
            logger.warn("couldn't create a connection, bailing out: " + ex.getMessage());
        }


        // process options

        if (cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("indexer", options, true);
            quit(0);
        }

        boolean verbose = false;
        if (cmd.hasOption("verbose")) {
            verbose = true;
        }

        // ES options
        String[] hosts = new String[]{"0.0.0.0"};
        int port = 9300;
        String clusterName = "elasticsearch_mdma";
        int bulkSize = 3000;

        if (cmd.hasOption("host")) {
            hosts = cmd.getOptionValues("host");
        }
        if (cmd.hasOption("port")) {
            port = Integer.parseInt(cmd.getOptionValue("port"));
        }
        if (cmd.hasOption("cluster")) {
            clusterName = cmd.getOptionValue("cluster");
        }
        if (cmd.hasOption("bulksize")) {
            bulkSize = Integer.parseInt(cmd.getOptionValue("bulksize"));
            if (bulkSize < 1 || bulkSize > 100000) {
                logger.error("bulksize must be between 1 and 100,000");
                quit(1);
            }
        }

        // ES Client
        final Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch_mdma").build();
        final TransportClient client = new TransportClient(settings);
        for (String host : hosts) {
            client.addTransportAddress(new InetSocketTransportAddress(host, port));
        }

        if (cmd.hasOption("filename") && cmd.hasOption("index") && cmd.hasOption("doctype")) {

            final String filename = cmd.getOptionValue("filename");

            final File _file = new File(filename);
            if (_file.length() == 0) {
                logger.info(_file.getAbsolutePath() + " is empty, skipping");
                quit(0); // file is empty
            }
            
            // for flat mode: leave a stampfile beside the json to 
            // indicate previous successful processing
            File directory = new File(filename).getParentFile();
            File stampfile = new File(directory, DigestUtils.shaHex(filename) + ".indexed");

            long start = System.currentTimeMillis();
            long lineCount = 0;

            final String indexName = cmd.getOptionValue("index");
            final String docType = cmd.getOptionValue("doctype");
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            
            try {

                final String srcSHA1 = extractSrcSHA1(filename);

                logger.debug(filename + " srcsha1: " + srcSHA1);

                long docsInIndex = getIndexedRecordCount(client, indexName, srcSHA1);
                logger.debug(filename + " indexed: " + docsInIndex);

                long docsInFile = getLineCount(filename);
                logger.debug(filename + " lines: " + docsInFile);

                if (cmd.hasOption("flat")) {
                    // flat mode
                    // .........
                    if (stampfile.exists()) {
                        logger.info("SKIPPING, since it seems this file has already "
                                + "been imported (found: " + stampfile.getAbsolutePath() + ")");
                        quit(0);
                    }
                } else {
                    // in non-flat-mode, indexing would take care
                    // of inconsistencies
                    if (docsInIndex == docsInFile) {
                        logger.info("UP-TO DATE: " + filename + " (" + docsInIndex + ", " + srcSHA1 + ")");
                        client.close();
                        quit(0);
                    }

                    if (docsInIndex > 0) {
                        logger.warn("INCONSISTENCY DETECTED: " + filename + ": indexed:" + docsInIndex
                                + " lines:" + docsInFile);

                        if (!cmd.hasOption("r")) {
                            logger.warn("Please re-run indexer with --repair flag or delete residues first with: $ curl -XDELETE " + hosts[0] + ":9200/" + indexName
                                    + "/_query -d ' {\"term\" : { \"meta.srcsha1\" : \"" + srcSHA1 + "\" }}'");
                            client.close();
                            quit(1);
                        } else {
                            logger.info("Attempting to clear residues...");
                            // attempt to repair once
                            DeleteByQueryResponse dbqr = client
                                    .prepareDeleteByQuery(indexName)
                                    .setQuery(termQuery("meta.srcsha1", srcSHA1))
                                    .execute()
                                    .actionGet();

                            Iterator<IndexDeleteByQueryResponse> it = dbqr.iterator();
                            long deletions = 0;
                            while (it.hasNext()) {
                                IndexDeleteByQueryResponse response = it.next();
                                deletions += 1;
                            }
                            logger.info("Deleted residues of " + filename);
                            logger.info("Refreshing [" + indexName + "]");
                            RefreshResponse refreshResponse =
                                    client.admin().indices().refresh(new RefreshRequest(indexName)).actionGet();

                            long indexedAfterDelete = getIndexedRecordCount(client, indexName, srcSHA1);
                            logger.info(indexedAfterDelete + " docs remained");
                            if (indexedAfterDelete > 0) {
                                logger.warn("Not all residues cleaned. Try to fix this manually: $ curl -XDELETE " + hosts[0] + ":9200/" + indexName
                                        + "/_query -d ' {\"term\" : { \"meta.srcsha1\" : \"" + srcSHA1 + "\" }}'");
                                quit(1);
                            } else {
                                logger.info("Residues are gone. Now trying to reindex: " + filename);
                            }
                        }
                    }
                }

                logger.info("INDEXING-REQUIRED: " + filename + " (" + docsInFile + ", " + srcSHA1 + ")");
                if (cmd.hasOption("status")) {
                    quit(0);
                }

                HashSet idsInBatch = new HashSet();

                String idField = null;
                if (cmd.hasOption("z")) {
                    idField = cmd.getOptionValue("z");
                }

                final FileReader fr = new FileReader(filename);
                final BufferedReader br = new BufferedReader(fr);

                String line;
                // one line is one document
                while ((line = br.readLine()) != null) {



                    // "Latest-Flag" machine
                    // This gets obsolete with a "flat" index
                    if (cmd.hasOption("z")) {
                        // flag that indicates, whether the document
                        // about to be indexed will be the latest
                        boolean willBeLatest = true;

                        // check if there is a previous (lower meta.timestamp) document with 
                        // the same identifier (whatever that may be - queried under "content")
                        final String contentIdentifier =
                                getContentIdentifier(line, idField);
                        idsInBatch.add(contentIdentifier);

                        // assumed in meta.timestamp
                        final Long timestamp =
                                Long.parseLong(getTimestamp(line));

                        logger.debug("Checking whether record is latest (line: " + lineCount + ")");
                        logger.debug(contentIdentifier + ", " + timestamp);

                        // get all docs, which match the contentIdentifier
                        // by filter, which doesn't score
                        final TermFilterBuilder idFilter = new TermFilterBuilder("content." + idField, contentIdentifier);
                        final TermFilterBuilder kindFilter = new TermFilterBuilder("meta.kind", docType);
                        final AndFilterBuilder afb = new AndFilterBuilder();
                        afb.add(idFilter).add(kindFilter);
                        final FilteredQueryBuilder fb = filteredQuery(matchAllQuery(), afb);

                        final SearchResponse searchResponse = client.prepareSearch(indexName)
                                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                                .setQuery(fb)
                                .setFrom(0)
                                .setSize(1200) // 3 years and 105 days assuming daily updates at the most
                                .setExplain(false).execute().actionGet();

                        final SearchHits searchHits = searchResponse.getHits();

                        logger.debug("docs with this id in the index: " + searchHits.getTotalHits());

                        for (final SearchHit hit : searchHits.getHits()) {
                            final String docId = hit.id();
                            final Map<String, Object> source = hit.sourceAsMap();
                            final Map meta = (Map) source.get("meta");
                            final Long docTimestamp = Long.parseLong(meta.get("timestamp").toString());
                            // if the indexed doc timestamp is lower the the current one, 
                            // remove any latest flag
                            if (timestamp >= docTimestamp) {
                                source.remove("latest");
                                final ObjectMapper mapper = new ObjectMapper();
                                // put the updated doc back
                                // IndexResponse response = 
                                client.prepareIndex(indexName, docType)
                                        .setCreate(false)
                                        .setId(docId)
                                        .setSource(mapper.writeValueAsBytes(source))
                                        .execute(new ActionListener<IndexResponse>() {
                                    public void onResponse(IndexResponse rspns) {
                                        logger.debug("Removed latest flag from "
                                                + contentIdentifier + ", " + docTimestamp
                                                + ", " + hit.id() + " since (" + timestamp + " > " + docTimestamp + ")");
                                    }

                                    public void onFailure(Throwable thrwbl) {
                                        logger.error("Could not remove flag from " + hit.id() + ", " + contentIdentifier);
                                    }
                                });
                                // .execute()
                                //.actionGet();
                            } else {
                                logger.debug("Doc " + hit.id() + " is newer (" + docTimestamp + ")");
                                willBeLatest = false;
                            }
                        }

                        if (willBeLatest) {
                            line = setLatestFlag(line);
                            logger.info("Setting latest flag on " + contentIdentifier + ", " + timestamp);
                        }

                        // end of latest-flag machine
                        // beware - this will be correct as long as there
                        // are no dups within one bulk!
                    }



                    bulkRequest.add(client.prepareIndex(indexName, docType).setSource(line));
                    lineCount++;
                    logger.debug("Added line " + lineCount + " to BULK");
                    logger.debug(line);

                    if (lineCount % bulkSize == 0) {

                        if (idsInBatch.size() != bulkSize && cmd.hasOption("z")) {
                            logger.error("This batch has duplications in the ID. That's not bad for the index, just makes the latest flag fuzzy");
                            logger.error("Bulk size was: " + bulkSize + ", but " + idsInBatch.size() + " IDs (only)");
                        }
                        idsInBatch.clear();

                        logger.debug("Issuing BULK request");

                        final long actionCount = bulkRequest.numberOfActions();
                        final BulkResponse bulkResponse = bulkRequest.execute().actionGet();
                        final long tookInMillis = bulkResponse.getTookInMillis();

                        if (bulkResponse.hasFailures()) {
                            logger.fatal("FAILED, bulk not indexed. exiting now.");
                            Iterator<BulkItemResponse> it = bulkResponse.iterator();
                            while (it.hasNext()) {
                                BulkItemResponse bir = it.next();
                                if (bir.isFailed()) {
                                    Failure failure = bir.getFailure();
                                    logger.fatal("id: " + failure.getId() + ", message: "
                                            + failure.getMessage() + ", type: "
                                            + failure.getType() + ", index: " + failure.getIndex());
                                }
                            }
                            quit(1);
                        } else {
                            if (verbose) {
                                final double elapsed = System.currentTimeMillis() - start;
                                final double speed = (lineCount / elapsed * 1000);
                                logger.info("OK (" + filename + ") " + lineCount + " docs indexed ("
                                        + actionCount + "/"
                                        + tookInMillis + "ms" + "/"
                                        + String.format("%.2f", speed) + "r/s)");
                            }
                        }
                        bulkRequest = client.prepareBulk();
                    }
                }

                // handle the remaining items
                final long actionCount = bulkRequest.numberOfActions();
                if (actionCount > 0) {
                    final BulkResponse bulkResponse = bulkRequest.execute().actionGet();
                    final long tookInMillis = bulkResponse.getTookInMillis();

                    if (bulkResponse.hasFailures()) {
                        logger.fatal("FAILED, bulk not indexed. exiting now.");
                        Iterator<BulkItemResponse> it = bulkResponse.iterator();
                        while (it.hasNext()) {
                            BulkItemResponse bir = it.next();
                            if (bir.isFailed()) {
                                Failure failure = bir.getFailure();
                                logger.fatal("id: " + failure.getId() + ", message: "
                                        + failure.getMessage() + ", type: "
                                        + failure.getType() + ", index: " + failure.getIndex());
                            }
                        }
                        quit(1);
                    } else {

                        // trigger update now
                        RefreshResponse refreshResponse =
                                client.admin().indices().refresh(new RefreshRequest(indexName)).actionGet();

                        if (verbose) {
                            final double elapsed = System.currentTimeMillis() - start;
                            final double speed = (lineCount / elapsed * 1000);
                            logger.info("OK (" + filename + ") " + lineCount + " docs indexed ("
                                    + actionCount + "/"
                                    + tookInMillis + "ms" + "/"
                                    + String.format("%.2f", speed) + "r/s)");
                        }

                    }

                }

                br.close();
                client.close();
                final double elapsed = (System.currentTimeMillis() - start) / 1000;
                final double speed = (lineCount / elapsed);
                logger.info("indexing (" + filename + ") " + lineCount + " docs took " + elapsed + "s (speed: " + String.format("%.2f", speed) + "r/s)");
                if (cmd.hasOption("flat")) {
                    try {
                        FileUtils.touch(stampfile);    
                    } catch (IOException ioe) {
                        logger.warn(".indexed files not created. Will reindex everything everytime.");
                    }
                }
            } catch (IOException e) {
                client.close();
                logger.error(e);
                quit(1);
            } finally {
                client.close();
            }
        }
        quit(0);
    }
}
