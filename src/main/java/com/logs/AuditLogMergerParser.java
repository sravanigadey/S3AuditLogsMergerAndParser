package com.logs;

import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * AuditLogMergerParser class will merge all audit logs into single file called 'AuditLogFile'
 * and also parse each and every audit log in it into key-value pairs
 * and finally convert the key-value pairs into csv file
 * and also into avro file to run queries on avro data using hive and spark to analyze the logs
 */
public class AuditLogMergerParser {

    private static Logger LOG = Logger.getLogger(AuditLogMergerParser.class);
    
    public static void main(String args[]) throws IOException {
        long startTime = System.currentTimeMillis();

        //executes the code in Merger class to get a file containing all audit logs
        Merger merger = new Merger();
        String auditLogsDirectoryPath = "/Users/sravani.gadey/Downloads/del";
        merger.mergeFiles(auditLogsDirectoryPath);


        //executes the code in S3LogParser class, which will parse the audit logs
        //and convert the key-value pairs into csv file and also avro file
        S3LogParser s3LogParser = new S3LogParser();
        String auditLogsFilePath = "AuditLogFile";
        s3LogParser.parseWholeAuditLog(auditLogsFilePath);

        //calculates the time required for the whole process of merging, parsing and converting into csv file and avro file
        long timeTaken = System.currentTimeMillis() - startTime;
        LOG.info("Time taken for merging, parsing and converting into file formats : " + timeTaken);
    }
}
