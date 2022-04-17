package com.logs;

import java.io.*;
import java.util.Scanner;

/**
 * Merger class merges all the audit logs present in a directory of multiple audit log files into a single audit log file
 */

public class Merger {
    public static void main() throws IOException {
//        Scanner sc = new Scanner(System.in);
//        System.out.println("Enter directory path to merge: ");
//        String pathname = sc.nextLine();
        File dir = new File("/Users/sravani.gadey/Downloads/del");
        PrintWriter pw = new PrintWriter("AuditLogFile");
        String[] fileNames = dir.list();

        /**
         * Reads each audit log file present in directory and writes each and every audit log from each file into a single audit log file
         */
        for (String fileName : fileNames) {
            //System.out.println("Reading from " + fileName);
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
        System.out.println("Completed reading from all files" + " in directory '" + dir.getName() + "'");
    }
}
