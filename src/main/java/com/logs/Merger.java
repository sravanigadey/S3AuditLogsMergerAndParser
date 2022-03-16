package com.logs;

import java.io.*;

public class Merger {
    public static void main(String[] args) throws IOException {
        File dir = new File(args[0]);
        PrintWriter pw = new PrintWriter("AuditLogFile");
        String[] fileNames = dir.list();
        long start = System.currentTimeMillis();

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
        long timeTaken = System.currentTimeMillis() - start;
        System.out.println(timeTaken);
        System.out.println("Reading from all files" + " in directory " + dir.getName() + " Completed");
    }
}
