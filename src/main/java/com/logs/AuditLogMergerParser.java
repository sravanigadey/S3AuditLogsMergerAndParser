package com.logs;

import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * AuditLogMergerParser class will merge all audit logs into single file
 * and also parse each and every audit log in it
 * and finally convert key-value pairs into csv file
 */
public class AuditLogMergerParser {

    static Logger LOG = Logger.getLogger(AuditLogMergerParser.class);
    
    public static void main(String args[]) throws IOException {
        long startTime = System.currentTimeMillis();

        /**
         * executes the merging code
         */
        Merger merger = new Merger();
        String auditLogsDirectoryPath = "/Users/sravani.gadey/Downloads/del";
        merger.mergeFiles(auditLogsDirectoryPath);

        /**
         * executes the parsing and converting into csv file code
         */
        S3LogParser parser = new S3LogParser();
        String auditLogsFilePath = "src/main/java/com/logs/AuditLogFile";
        parser.parseWholeAuditLog(auditLogsFilePath);

        /**
         * used to calculate the time required for the whole process of merging, parsing and converting into csv file
         */
        long timeTaken = System.currentTimeMillis() - startTime;
        LOG.info("Time taken for merging and parsing : " + timeTaken);
    }
}
