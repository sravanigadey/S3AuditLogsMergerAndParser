package com.logs;

import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.*;

public class MergerTest {

    public MergerTest() throws IOException {
    }

    File auditLogFile = new File("AuditLogFile");

    @Before
    public void setUp() throws Exception {
        String dirPath = "samplefiles";
        File directory = new File(dirPath);
        directory.mkdir();
        String filename1 = "samplefiles/file1.txt";
        File file1 = new File(filename1);
        file1.createNewFile();
        FileWriter fw = new FileWriter(file1);
        fw.write("abcd");
        fw.close();
        String filename2 = "samplefiles/file2.txt";
        File file2 = new File(filename2);
        file2.createNewFile();
        FileWriter fw1 = new FileWriter(file2);
        fw1.write("efgh");
        fw1.close();
        String filename3 = "samplefiles/file3.txt";
        File file3 = new File(filename3);
        file3.createNewFile();
        FileWriter fw2 = new FileWriter(file3);
        fw2.write("ijkl");
        fw2.close();
        String emptyDirPath = "emptyDir";
        File emptyDirectory = new File(emptyDirPath);
        emptyDirectory.mkdir();
    }

    @Test
    public void mergeFilesTest() throws IOException {
        Merger merger = new Merger();
        merger.mergeFiles("samplefiles");
        String str = new String(Files.readAllBytes(Paths.get(String.valueOf(auditLogFile))));
        //System.out.println(str);
        String fileText = str.replace("\n", "");
        //System.out.println(fileText);
        assertTrue(fileText.contains("abcd"));
    }

    @Test
    public void mergeFilesTestEmpty() throws IOException {
        Merger merger = new Merger();
        merger.mergeFiles("emptyDir");
        boolean fileExists = auditLogFile.exists();
        //System.out.println(fileExists);
        //assertTrue(fileExists);
        assertFalse(fileExists);
    }

}