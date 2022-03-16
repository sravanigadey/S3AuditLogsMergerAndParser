package com.logs;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//import org.apache.hadoop.classification.InterfaceAudience;
//import org.apache.hadoop.classification.InterfaceStability;

/**
 * Class to help parse AWS S3 Logs.
 * see https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html
 *
 * Getting the regexp right is surprisingly hard; this class does it
 * explicitly and names each group in the process.
 * All group names are included in {@link #AWS_LOG_REGEXP_GROUPS} in the order
 * within the log entries.
 */
//@InterfaceAudience.Public
//@InterfaceStability.Unstable
public final class S3LogParser {

    private S3LogParser() {
    }

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

    public static void main(String[] args) {
        System.out.println("Matcher pattern is " + LOG_ENTRY_PATTERN);
        System.out.println("Log entry is " + SAMPLE_LOG_ENTRY);
        final Matcher matcher = LOG_ENTRY_PATTERN.matcher(SAMPLE_LOG_ENTRY);
        System.out.println(matcher);
        System.out.println(AWS_LOG_REGEXP_GROUPS);
        //System.out.println(matcher.group("owner"));
        for (String name : AWS_LOG_REGEXP_GROUPS) {
            try {
                final String grp = matcher.group(name);
                System.out.println("[{" + name + "}]: '{" + grp + "}'");
            } catch (IllegalStateException e) {
                System.out.println(e);
            }
        }
    }
}
