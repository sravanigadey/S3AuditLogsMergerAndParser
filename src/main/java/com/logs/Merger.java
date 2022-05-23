package com.logs;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.Arrays;

/**
 * Merger class will merge all the audit logs present in a directory of multiple audit log files into a single audit log file
 */

public class Merger {

    Logger LOG = Logger.getLogger(AuditLogMergerParser.class);

    void mergeFiles(String auditLogsDirectoryPath) throws IOException {
        File auditLogFilesDirectory = new File(auditLogsDirectoryPath);
        String[] auditLogFileNames = auditLogFilesDirectory.list();
        //LOG.info(Arrays.toString(auditLogFileNames));

        /**
         * Read each audit log file present in directory and writes each and every audit log in it into a single audit log file
         */
        if(auditLogFileNames != null && auditLogFileNames.length != 0) {
            File auditLogFile = new File("AuditLogFile");
            PrintWriter printWriter = new PrintWriter(auditLogFile);
            for (String singleFileName : auditLogFileNames) {
                //LOG.info("Reading from " + fileName);
                File file = new File(auditLogFilesDirectory, singleFileName);
                FileReader fileReader = new FileReader(file);
                BufferedReader bufferedReader = new BufferedReader(fileReader);
                //printWriter.println("Contents of file " + fileName);
                String singleLine = bufferedReader.readLine();
                while (singleLine != null) {
                    printWriter.println(singleLine);
                    singleLine = bufferedReader.readLine();
                }
                printWriter.flush();
            }
            //LOG.info("Completed reading from all files" + " in directory '" + dir.getName() + "'");
        }
    }
}
