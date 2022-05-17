package com.logs;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class S3LogParserTest {

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

    public static final String REFERRER_ENTRY =
            "op=op_create"
                    + "&p1=fork-0001/test/testParseBrokenCSVFile"
                    + "&pr=alice"
                    + "&ps=2eac5a04-2153-48db-896a-09bc9a2fd132"
                    + "&id=e8ede3c7-8506-4a43-8268-fe8fcbb510a4-00000278&t0=154"
                    + "&fs=e8ede3c7-8506-4a43-8268-fe8fcbb510a4&t1=156"
                    + "&ts=1620905165700";

    S3LogParser s3LogParser = new S3LogParser();

    @Before
    public void setUp() throws Exception {
        String filename = "exampleauditlogfile.txt";
        File file = new File(filename);
        file.createNewFile();
        FileWriter fw = new FileWriter(file);
        fw.write(SAMPLE_LOG_ENTRY);
        fw.close();
        String emptyfilename = "emptyfile.txt";
        File emptyFile = new File(emptyfilename);
        emptyFile.createNewFile();
    }

    @Test
    public void parseAuditLog() {
        Map<String, String> parseAuditLogResult = s3LogParser.parseAuditLog(SAMPLE_LOG_ENTRY);
        //System.out.println(parseAuditLogResult);
        assertEquals("bucket-london", parseAuditLogResult.get("bucket"));
        assertEquals("109.157.171.174", parseAuditLogResult.get("remoteip"));
    }

    @Test
    public void parseReferrerHeader() {
        Map<String, String> parseReferrerHeaderResult = s3LogParser.parseReferrerHeader(REFERRER_ENTRY);
        //System.out.println(parseReferrerHeaderResult);
        assertEquals("fork-0001/test/testParseBrokenCSVFile", parseReferrerHeaderResult.get("p1"));
        assertEquals("alice", parseReferrerHeaderResult.get("pr"));
    }

    @Test
    public void parseWholeAuditLogTest() throws IOException {
        File sampleFile = new File("exampleauditlogfile.txt");
        List<HashMap<String, String>> parseWholeAuditLogResult = s3LogParser.parseWholeAuditLog(String.valueOf(sampleFile));
        //System.out.println(parseWholeAuditLogResult);
        assertEquals("op_create", parseWholeAuditLogResult.get(0).get("op"));
        assertEquals("REST.PUT.OBJECT", parseWholeAuditLogResult.get(0).get("verb"));
        assertEquals("\"https://audit.example.org/hadoop/1/op_create/e8ede3c7-8506-4a43-8268-fe8fcbb510a4-00000278/?op=op_create&p1=fork-0001/test/testParseBrokenCSVFile&pr=alice&ps=2eac5a04-2153-48db-896a-09bc9a2fd132&id=e8ede3c7-8506-4a43-8268-fe8fcbb510a4-00000278&t0=154&fs=e8ede3c7-8506-4a43-8268-fe8fcbb510a4&t1=156&ts=1620905165700\"", parseWholeAuditLogResult.get(0).get("referrer"));
    }

    @Test
    public void parseWholeAuditLogTestEmpty() throws IOException {
        File emptyFile = new File("/Users/sravani.gadey/Downloads/emptyfile.txt");
        List<HashMap<String, String>> parseWholeAuditLogResult = s3LogParser.parseWholeAuditLog(String.valueOf(emptyFile));
        //System.out.println(emptyFile.getPath());
        //System.out.println(parseWholeAuditLogResult.isEmpty());
        assertTrue(parseWholeAuditLogResult.isEmpty());
    }
}