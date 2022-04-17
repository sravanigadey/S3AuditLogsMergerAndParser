package com.logs;

import java.io.IOException;

/**
 * AuditLogMergerParser class will merge all audit logs into single file
 * and also parse each and every audit log in it
 * and finally convert key-value pairs into csv file
 */
public class AuditLogMergerParser {
    public static void main(String args[]) throws IOException {
        long startTime = System.currentTimeMillis();

        /**
         * executes the merging code
         */
        Merger.main();

        /**
         * executes the parsing and converting into csv file code
         */
        S3LogParser.main();

        /**
         * used to calculate the time required for the whole process of merging, parsing and converting into csv file
         */
        long timeTaken = System.currentTimeMillis() - startTime;
        System.out.println("Time taken for merging and parsing : " + timeTaken);
    }
}
