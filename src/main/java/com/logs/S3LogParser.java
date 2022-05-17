package com.logs;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.log4j.Logger;

/**
 * Class to help parse AWS S3 Logs.
 * see https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html
 *
 * Getting the regexp right is surprisingly hard; this class does it
 * explicitly and names each group in the process.
 * All group names are included in {@link #AWS_LOG_REGEXP_GROUPS} in the order
 * within the log entries.
 */

public class S3LogParser {

    public S3LogParser() {
    }

    Logger LOG = Logger.getLogger(S3LogParser.class);

    /**
     * Simple entry: anything up to a space.
     * {@value}.
     */
    private static final String SIMPLE = "[^ ]*";

    /**
     * Date/Time. Everything within square braces.
     * {@value}.
     */
    private static final String DATETIME = "\\[(.*?)\\]";

    /**
     * A natural number or "-".
     * {@value}.
     */
    private static final String NUMBER = "(-|[0-9]*)";

    /**
     * A Quoted field or "-".
     * {@value}.
     */
    private static final String QUOTED = "(-|\"[^\"]*\")";

    /**
     * An entry in the regexp.
     *
     * @param name    name of the group
     * @param pattern pattern to use in the regexp
     * @return the pattern for the regexp
     */
    private static String e(String name, String pattern) {
        return String.format("(?<%s>%s) ", name, pattern);
    }

    /**
     * An entry in the regexp.
     *
     * @param name    name of the group
     * @param pattern pattern to use in the regexp
     * @return the pattern for the regexp
     */
    private static String eNoTrailing(String name, String pattern) {
        return String.format("(?<%s>%s)", name, pattern);
    }

    /**
     * Simple entry using the {@link #SIMPLE} pattern.
     *
     * @param name name of the element (for code clarity only)
     * @return the pattern for the regexp
     */
    private static String e(String name) {
        return e(name, SIMPLE);
    }

    /**
     * Quoted entry using the {@link #QUOTED} pattern.
     *
     * @param name name of the element (for code clarity only)
     * @return the pattern for the regexp
     */
    private static String q(String name) {
        return e(name, QUOTED);
    }

    /**
     * Log group {@value}.
     */
    public static final String OWNER_GROUP = "owner";

    /**
     * Log group {@value}.
     */
    public static final String BUCKET_GROUP = "bucket";

    /**
     * Log group {@value}.
     */
    public static final String TIMESTAMP_GROUP = "timestamp";

    /**
     * Log group {@value}.
     */
    public static final String REMOTEIP_GROUP = "remoteip";

    /**
     * Log group {@value}.
     */
    public static final String REQUESTER_GROUP = "requester";

    /**
     * Log group {@value}.
     */
    public static final String REQUESTID_GROUP = "requestid";

    /**
     * Log group {@value}.
     */
    public static final String VERB_GROUP = "verb";

    /**
     * Log group {@value}.
     */
    public static final String KEY_GROUP = "key";

    /**
     * Log group {@value}.
     */
    public static final String REQUESTURI_GROUP = "requesturi";

    /**
     * Log group {@value}.
     */
    public static final String HTTP_GROUP = "http";

    /**
     * Log group {@value}.
     */
    public static final String AWSERRORCODE_GROUP = "awserrorcode";

    /**
     * Log group {@value}.
     */
    public static final String BYTESSENT_GROUP = "bytessent";

    /**
     * Log group {@value}.
     */
    public static final String OBJECTSIZE_GROUP = "objectsize";

    /**
     * Log group {@value}.
     */
    public static final String TOTALTIME_GROUP = "totaltime";

    /**
     * Log group {@value}.
     */
    public static final String TURNAROUNDTIME_GROUP = "turnaroundtime";

    /**
     * Log group {@value}.
     */
    public static final String REFERRER_GROUP = "referrer";

    /**
     * Log group {@value}.
     */
    public static final String USERAGENT_GROUP = "useragent";

    /**
     * Log group {@value}.
     */
    public static final String VERSION_GROUP = "version";

    /**
     * Log group {@value}.
     */
    public static final String HOSTID_GROUP = "hostid";

    /**
     * Log group {@value}.
     */
    public static final String SIGV_GROUP = "sigv";

    /**
     * Log group {@value}.
     */
    public static final String CYPHER_GROUP = "cypher";

    /**
     * Log group {@value}.
     */
    public static final String AUTH_GROUP = "auth";

    /**
     * Log group {@value}.
     */
    public static final String ENDPOINT_GROUP = "endpoint";

    /**
     * Log group {@value}.
     */
    public static final String TLS_GROUP = "tls";

    /**
     * This is where anything at the tail of a log
     * entry ends up; it is null unless/until the AWS
     * logs are enhanced in future.
     * Value {@value}.
     */
    public static final String TAIL_GROUP = "tail";

    /**
     * Construct the log entry pattern.
     */
    public static final String LOG_ENTRY_REGEXP = ""
            + e(OWNER_GROUP)
            + e(BUCKET_GROUP)
            + e(TIMESTAMP_GROUP, DATETIME)
            + e(REMOTEIP_GROUP)
            + e(REQUESTER_GROUP)
            + e(REQUESTID_GROUP)
            + e(VERB_GROUP)
            + e(KEY_GROUP)
            + q(REQUESTURI_GROUP)
            + e(HTTP_GROUP, NUMBER)
            + e(AWSERRORCODE_GROUP)
            + e(BYTESSENT_GROUP)
            + e(OBJECTSIZE_GROUP)
            + e(TOTALTIME_GROUP)
            + e(TURNAROUNDTIME_GROUP)
            + q(REFERRER_GROUP)
            + q(USERAGENT_GROUP)
            + e(VERSION_GROUP)
            + e(HOSTID_GROUP)
            + e(SIGV_GROUP)
            + e(CYPHER_GROUP)
            + e(AUTH_GROUP)
            + e(ENDPOINT_GROUP)
            + eNoTrailing(TLS_GROUP, SIMPLE)
            + eNoTrailing(TAIL_GROUP, ".*") // anything which follows
            + "$"; // end of line

    /**
     * Groups in order.
     */
    private static final String[] GROUPS = {
            OWNER_GROUP,
            BUCKET_GROUP,
            TIMESTAMP_GROUP,
            REMOTEIP_GROUP,
            REQUESTER_GROUP,
            REQUESTID_GROUP,
            VERB_GROUP,
            KEY_GROUP,
            REQUESTURI_GROUP,
            HTTP_GROUP,
            AWSERRORCODE_GROUP,
            BYTESSENT_GROUP,
            OBJECTSIZE_GROUP,
            TOTALTIME_GROUP,
            TURNAROUNDTIME_GROUP,
            REFERRER_GROUP,
            USERAGENT_GROUP,
            VERSION_GROUP,
            HOSTID_GROUP,
            SIGV_GROUP,
            CYPHER_GROUP,
            AUTH_GROUP,
            ENDPOINT_GROUP,
            TLS_GROUP,
            TAIL_GROUP
    };

    /**
     * Ordered list of regular expression group names.
     */
    public static final List<String> AWS_LOG_REGEXP_GROUPS =
            Collections.unmodifiableList(Arrays.asList(GROUPS));

    /**
     * And the actual compiled pattern.
     */
    public static final Pattern LOG_ENTRY_PATTERN = Pattern.compile(
            LOG_ENTRY_REGEXP);

    /**
     * A real log entry.
     * This is derived from a real log entry on a test run.
     * If this needs to be updated, please do it from a real log.
     * Splitting this up across lines has a tendency to break things, so
     * be careful making changes.
     */
    public static final String SAMPLE_LOG_ENTRY =
            "183c9826b45486e485693808f38e2c4071004bf5dfd4c3ab210f0a21a4000000"
                    + " bucket-london"
                    + " [13/May/2021:11:26:06 +0000]"
                    + " 109.157.171.174"
                    + " arn:aws:iam::152813717700:user/dev"
                    + " M7ZB7C4RTKXJKTM9"
                    + " REST.PUT.OBJECT"
                    + " fork-0001/test/testParseBrokenCSVFile"
                    + " \"PUT /fork-0001/test/testParseBrokenCSVFile HTTP/1.1\""
                    + " 200"
                    + " -"
                    + " -"
                    + " 794"
                    + " 55"
                    + " 17"
                    + " \"https://audit.example.org/hadoop/1/op_create/"
                    + "e8ede3c7-8506-4a43-8268-fe8fcbb510a4-00000278/"
                    + "?op=op_create"
                    + "&p1=fork-0001/test/testParseBrokenCSVFile"
                    + "&pr=alice"
                    + "&ps=2eac5a04-2153-48db-896a-09bc9a2fd132"
                    + "&id=e8ede3c7-8506-4a43-8268-fe8fcbb510a4-00000278&t0=154"
                    + "&fs=e8ede3c7-8506-4a43-8268-fe8fcbb510a4&t1=156&"
                    + "ts=1620905165700\""
                    + " \"Hadoop 3.4.0-SNAPSHOT, java/1.8.0_282 vendor/AdoptOpenJDK\""
                    + " -"
                    + " TrIqtEYGWAwvu0h1N9WJKyoqM0TyHUaY+ZZBwP2yNf2qQp1Z/0="
                    + " SigV4"
                    + " ECDHE-RSA-AES128-GCM-SHA256"
                    + " AuthHeader"
                    + " bucket-london.s3.eu-west-2.amazonaws.com"
                    + " TLSv1.2";

    /**
     * parseAuditLog method helps in parsing the audit log into key-value pairs using regular expression
     * @param singleAuditLog this is single audit log from merged audit log file
     * @return it returns a map i.e, auditLogMap which contains key-value pairs of a single audit log
     */
     Map<String, String> parseAuditLog(String singleAuditLog) {
        Map<String, String> auditLogMap = new HashMap<>();
        final Matcher matcher = LOG_ENTRY_PATTERN.matcher(singleAuditLog);
        matcher.matches();
        //LOG.info("auditlog : " + matcher.matches());
        for (String key : AWS_LOG_REGEXP_GROUPS) {
            try {
                final String value = matcher.group(key);
                auditLogMap.put(key, value);
                //LOG.info("[{" + name + "}]: '{" + grp + "}'");
            } catch (IllegalStateException e) {
                LOG.info("log : " + singleAuditLog);
                LOG.info(e);
            }
        }
        return auditLogMap;
    }

    /**
     * parseReferrerHeader method helps in parsing the http referrer header which is one of the key-value pair of audit log
     * @param referrerHeader this is the http referrer header of a particular audit log
     * @return it returns a map i.e, auditLogMap which contains key-value pairs of audit log as well as referrer header present in it
     */
    Map<String, String> parseReferrerHeader(String referrerHeader) {
         int indx = referrerHeader.indexOf("?");
         String httpreferrer = referrerHeader.substring(indx + 1, referrerHeader.length() - 1);
         int start = 0;
         int len = httpreferrer.length();
         Map<String, String> referrerHeaderMap = new HashMap<>();
         while (start < len) {
             int equals = httpreferrer.indexOf("=", start);
             // no match : break
             if (equals == -1) {
                 break;
             }
             // todo, handle equals == start
             String key = httpreferrer.substring(start, equals);
             int end = httpreferrer.indexOf("&", equals);
             // or end of string
             if (end == -1) {
                 end = len;
             }
             // todo, no value?
             String value = httpreferrer.substring(equals + 1, end);
             //LOG.info(key + ":" + value);
             referrerHeaderMap.put(key, value);
             start = end + 1;
         }
         return referrerHeaderMap;
    }

    /**
     * convertJsonToCsvFile method converts the json file into csv file
     * in which all key-value pairs of all audit logs are displayed as a table
     * @throws IOException
     */
     void convertJsonToCsvFile() throws IOException {
         JsonNode jsonTree = new ObjectMapper().readTree(new File("Json.json"));
         //LOG.info(jsonTree);

         CsvSchema.Builder csvSchemaBuilder = CsvSchema.builder();
         //LOG.info(csvSchemaBuilder);
         JsonNode firstObject = jsonTree.elements().next();
         //LOG.info(firstObject);
         firstObject.fieldNames().forEachRemaining(fieldName -> {csvSchemaBuilder.addColumn(fieldName);} );
         CsvSchema csvSchema = csvSchemaBuilder.build().withHeader();
         //LOG.info(csvSchema);

         File csvFile = new File("CsvLogs.csv");
         CsvMapper csvMapper = new CsvMapper();
         csvMapper.writerFor(JsonNode.class)
                .with(csvSchema)
                .writeValue(csvFile, jsonTree);
         //LOG.info(csvMapper);
    }

    void serialize(List<HashMap<String, String>> referrerHeaderList, List<HashMap<String, String>> auditLogList) throws IOException {
        //Instantiating the Schema.Parser class.
        Schema schema = new Schema.Parser().parse(new File("/Users/sravani.gadey/Desktop/Workstation/S3AuditLogsMergerAndParser/src/main/java/com/logs/schema.avsc"));
        //LOG.info("Schema: " + schema);

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);

        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        File avroFile = new File("data.avro");
        dataFileWriter.create(schema, avroFile);

        ArrayList<String> longValues = new ArrayList<>(Arrays.asList("turnaroundtime", "bytessent", "objectsize", "totaltime"));
        int count = 0;

        //Insert data according to schema
        for(Map<String,String> mp : auditLogList) {
            //Instantiating the GenericRecord class.
            GenericRecord e1 = new GenericData.Record(schema);

            for (Map.Entry<String,String> entry : mp.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue().trim();

                //if value == '-' and key is not in arraylist then put '-' or else '-1'
                //if key is in arraylist of long values then parse the long value
                //while parsing do it in try-catch block, in catch block need to log exception and set value as '-1'
                try {
                    if(longValues.contains(key)) {
                        if(value.equals("-")) {
                            e1.put(key, null);
                        }
                        else {
                            e1.put(key, Long.parseLong(value));
                        }
                    }
                    else {
                        e1.put(key, value);
                    }
                }
                catch (Exception e) {
                    LOG.info("Exception : " + e);
                    e1.put(key, null);
                }
            }
            e1.put("referrerMap", referrerHeaderList.get(count));
            dataFileWriter.append(e1);
            count += 1;
        }
        dataFileWriter.close();

        //LOG.info("data successfully serialized");
    }

    List<HashMap<String, String>> parseWholeAuditLog(String auditLogsFilePath) throws IOException {
        List<HashMap<String, String>> entireAuditLogList = new ArrayList<>();
        List<HashMap<String, String>> referrerHeaderList = new ArrayList<>();
        List<HashMap<String, String>> auditLogList = new ArrayList<>();
        File f = new File(auditLogsFilePath);

        if(f.length() != 0) {
            File file = new File("Json.json");
            BufferedReader br = new BufferedReader(new FileReader(f));
            String singleAuditLog;
            ObjectMapper mapper = new ObjectMapper();

            /**
             * reads single audit log from merged audit log file and parse it
             */
            while ((singleAuditLog = br.readLine()) != null) {
                Map<String, String> auditLogMap = parseAuditLog(singleAuditLog);
                //LOG.info("auditLogMap : " + auditLogMap);

                String referrerHeader = auditLogMap.get("referrer");
                if (referrerHeader == null || referrerHeader.equals("-")) {
                    //LOG.info("Log didn't parsed : " + referrerHeader);
                    continue;
                }
                //LOG.info("getref : "+ referrerHeader);

                Map<String, String> referrerHeaderMap = parseReferrerHeader(referrerHeader);
                //LOG.info("referrerHeaderMap : " + referrerHeaderMap);
                Map<String, String> entireAuditLogMap = new HashMap<>();
                entireAuditLogMap.putAll(auditLogMap);
                entireAuditLogMap.putAll(referrerHeaderMap);
                //LOG.info("entireAuditLogMap : " + entireAuditLogMap);

                /**
                 * adds every single map containing key-value pairs of single audit log into a list
                 */
                auditLogList.add((HashMap<String, String>) auditLogMap);
                referrerHeaderList.add((HashMap<String, String>) referrerHeaderMap);
                entireAuditLogList.add((HashMap<String, String>) entireAuditLogMap);

                //LOG.info("Parsed one log..........");
            }
            serialize(referrerHeaderList, auditLogList);

            /**
             * adds list into json file which helps to convert key-value pairs into csv file
             */
            mapper.writeValue(file, entireAuditLogList);

            /**
             * this method is used to convert the obtained json file into csv file
             */
            convertJsonToCsvFile();
        }
        return entireAuditLogList;
    }

}
