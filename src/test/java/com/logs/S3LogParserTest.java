package com.logs;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * S3LogParserTest will implement different tests on S3LogParser class methods
 */
public class S3LogParserTest {

    /**
     * A real log entry.
     * This is derived from a real log entry on a test run.
     * If this needs to be updated, please do it from a real log.
     * Splitting this up across lines has a tendency to break things, so
     * be careful making changes.
     */
    String SAMPLE_LOG_ENTRY =
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
     * A real referrer header entry.
     * This is derived from a real log entry on a test run.
     * If this needs to be updated, please do it from a real log.
     * Splitting this up across lines has a tendency to break things, so
     * be careful making changes.
     */
    public static final String SAMPLE_REFERRER_ENTRY =
            "op=op_create"
                    + "&p1=fork-0001/test/testParseBrokenCSVFile"
                    + "&pr=alice"
                    + "&ps=2eac5a04-2153-48db-896a-09bc9a2fd132"
                    + "&id=e8ede3c7-8506-4a43-8268-fe8fcbb510a4-00000278&t0=154"
                    + "&fs=e8ede3c7-8506-4a43-8268-fe8fcbb510a4&t1=156"
                    + "&ts=1620905165700";

    S3LogParser s3LogParser = new S3LogParser();

    /**
     * sample directories and files to test
     */
    File sampleAuditLogFile = new File("sampleauditlogfile.txt");
    File sampleAuditLogFileDiffDir = new File("/Users/sravani.gadey/Downloads/sampleauditlogfile.txt");
    File emptyFile = new File("emptyfile.txt");
    File emptyFileDiffDir = new File("/Users/sravani.gadey/Downloads/emptyfile.txt");
    File emptyDirectory = new File("emptyDir");

    /**
     * creates the sample directories and files before each test
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        sampleAuditLogFile.createNewFile();
        FileWriter fw = new FileWriter(sampleAuditLogFile);
        fw.write(SAMPLE_LOG_ENTRY);
        fw.close();
        sampleAuditLogFileDiffDir.createNewFile();
        FileWriter fw1 = new FileWriter(sampleAuditLogFileDiffDir);
        fw1.write(SAMPLE_LOG_ENTRY);
        fw1.close();
        emptyFile.createNewFile();
        emptyFileDiffDir.createNewFile();
        emptyDirectory.mkdir();
    }

    /**
     * parseAuditLogTest() will test parseAuditLog() method
     * by passing sample audit log entry
     * and checks if the log is parsed correctly
     */
    @Test
    public void parseAuditLogTest() {
        Map<String, String> parseAuditLogResult = s3LogParser.parseAuditLog(SAMPLE_LOG_ENTRY);
        //System.out.println(parseAuditLogResult);
        assertEquals("bucket-london", parseAuditLogResult.get("bucket"));
        assertEquals("109.157.171.174", parseAuditLogResult.get("remoteip"));
    }

    /**
     * parseReferrerHeaderTest() will test parseReferrerHeader() method
     * by passing sample referrer header taken from sample audit log
     * and checks if the referrer header is parsed correctly
     */
    @Test
    public void parseReferrerHeaderTest() {
        Map<String, String> parseReferrerHeaderResult = s3LogParser.parseReferrerHeader(SAMPLE_REFERRER_ENTRY);
        //System.out.println(parseReferrerHeaderResult);
        assertEquals("fork-0001/test/testParseBrokenCSVFile", parseReferrerHeaderResult.get("p1"));
        assertEquals("alice", parseReferrerHeaderResult.get("pr"));
    }

    /**
     * parseWholeAuditLogTest() will test parseWholeAuditLog() method
     * by passing sample file which contains a single audit log
     * and checks if key-value pairs are parsed correctly
     * @throws IOException
     */
    @Test
    public void parseWholeAuditLogTest() throws IOException {
        List<HashMap<String, String>> parseWholeAuditLogTestResult = s3LogParser.parseWholeAuditLog(sampleAuditLogFile.getPath());
        //System.out.println(parseWholeAuditLogTestResult);
        assertEquals("op_create", parseWholeAuditLogTestResult.get(0).get("op"));
        assertEquals("REST.PUT.OBJECT", parseWholeAuditLogTestResult.get(0).get("verb"));
        assertEquals("\"https://audit.example.org/hadoop/1/op_create/e8ede3c7-8506-4a43-8268-fe8fcbb510a4-00000278/?op=op_create&p1=fork-0001/test/testParseBrokenCSVFile&pr=alice&ps=2eac5a04-2153-48db-896a-09bc9a2fd132&id=e8ede3c7-8506-4a43-8268-fe8fcbb510a4-00000278&t0=154&fs=e8ede3c7-8506-4a43-8268-fe8fcbb510a4&t1=156&ts=1620905165700\"", parseWholeAuditLogTestResult.get(0).get("referrer"));
    }

    /**
     * parseWholeAuditLogEmptyFileTest() will test parseWholeAuditLog() method
     * by passing an empty file which doesn't contains any data
     * and checks if return result is empty or not
     * @throws IOException
     */
    @Test
    public void parseWholeAuditLogEmptyFileTest() throws IOException {
        List<HashMap<String, String>> parseWholeAuditLogEmptyFileTestResult = s3LogParser.parseWholeAuditLog(emptyFile.getPath());
        //System.out.println(parseWholeAuditLogTestEmptyResult.isEmpty());
        assertTrue(parseWholeAuditLogEmptyFileTestResult.isEmpty());
    }

    /**
     * parseWholeAuditLogEmptyDirTest() will test parseWholeAuditLog() method
     * by passing an empty directory and checks if it is a file or not
     * and checks if return result is empty or not
     * @throws IOException
     */
    @Test
    public void parseWholeAuditLogEmptyDirTest() throws IOException {
        List<HashMap<String, String>> parseWholeAuditLogEmptyDirTestResult = s3LogParser.parseWholeAuditLog(emptyDirectory.getPath());
        //System.out.println(parseWholeAuditLogTestEmptyDirResult);
        assertTrue(parseWholeAuditLogEmptyDirTestResult.isEmpty());
    }

    /**
     * delete all the sample directories and files after each test
     * @throws Exception
     */
    @After
    public void tearDown() throws Exception {
        sampleAuditLogFile.delete();
        sampleAuditLogFileDiffDir.delete();
        emptyFile.delete();
        emptyFileDiffDir.delete();
        emptyDirectory.delete();
    }
}