package com.logs;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.Arrays;

/**
 * Merger class will merge all the audit logs present in a directory of multiple audit log files into a single audit log file
 */

public class Merger {

    private final Logger LOG = Logger.getLogger(Merger.class);

    public void mergeFiles(String auditLogsDirectoryPath) throws IOException {
        File auditLogFilesDirectory = new File(auditLogsDirectoryPath);
        String[] auditLogFileNames = auditLogFilesDirectory.list();
        LOG.info("Files to be merged : " + Arrays.toString(auditLogFileNames));

        //Read each audit log file present in directory and writes each and every audit log in it into a single audit log file
        if(auditLogFileNames != null && auditLogFileNames.length != 0) {
            File auditLogFile = new File("AuditLogFile");
            try (PrintWriter printWriter = new PrintWriter(auditLogFile)) {
                for (String singleFileName : auditLogFileNames) {
                    File file = new File(auditLogFilesDirectory, singleFileName);
                    try (FileReader fileReader = new FileReader(file);
                         BufferedReader bufferedReader = new BufferedReader(fileReader)) {
                        String singleLine = bufferedReader.readLine();
                        while (singleLine != null) {
                            printWriter.println(singleLine);
                            singleLine = bufferedReader.readLine();
                        }
                        printWriter.flush();
                    }
                    LOG.info("Successfully merged all files from the directory '" + auditLogFilesDirectory.getName() + "'");
                }
            }
        }
    }
}
