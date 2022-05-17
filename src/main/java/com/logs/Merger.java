package com.logs;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.Arrays;

/**
 * Merger class merges all the audit logs present in a directory of multiple audit log files into a single audit log file
 */

public class Merger {

    Logger LOG = Logger.getLogger(AuditLogMergerParser.class);

    void mergeFiles(String auditLogsDirectoryPath) throws IOException {
//        Scanner sc = new Scanner(System.in);
//        LOG.info("Enter directory path to merge: ");
//        String pathname = sc.nextLine();
        File dir = new File(auditLogsDirectoryPath);
        String[] fileNames = dir.list();
        //LOG.info(Arrays.toString(fileNames));

        /**
         * Reads each audit log file present in directory and writes each and every audit log from each file into a single audit log file
         */
        if(fileNames != null && fileNames.length != 0) {
            File resultFile = new File("AuditLogFile");
            PrintWriter pw = new PrintWriter(resultFile);
            for (String fileName : fileNames) {
                //LOG.info("Reading from " + fileName);
                File f = new File(dir, fileName);
                FileReader fr = new FileReader(f);
                BufferedReader br = new BufferedReader(fr);
                //pw.println("Contents of file " + fileName);
                String line = br.readLine();
                while (line != null) {
                    pw.println(line);
                    line = br.readLine();
                }
                pw.flush();
            }
            //LOG.info("Completed reading from all files" + " in directory '" + dir.getName() + "'");
        }
    }
}
