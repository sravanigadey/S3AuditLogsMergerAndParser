package com.logs;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.*;

/**
 * MergerTest will implement different tests on Merger class methods
 */
public class MergerTest {

    /**
     * sample directories and files to test
     */
    File auditLogFile = new File("AuditLogFile");
    File sampleDirectory = new File("sampleFilesDirectory");
    File emptyDirectory = new File("emptyFilesDirectory");
    File firstSampleFile = new File("sampleFilesDirectory/sampleFile1.txt");
    File secondSampleFile = new File("sampleFilesDirectory/sampleFile2.txt");
    File thirdSampleFile = new File("sampleFilesDirectory/sampleFile3.txt");

    public MergerTest() throws IOException {
    }

    /**
     * creates the sample directories and files before each test
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        sampleDirectory.mkdir();
        firstSampleFile.createNewFile();
        FileWriter fw = new FileWriter(firstSampleFile);
        fw.write("abcd");
        fw.close();
        secondSampleFile.createNewFile();
        FileWriter fw1 = new FileWriter(secondSampleFile);
        fw1.write("efgh");
        fw1.close();
        thirdSampleFile.createNewFile();
        FileWriter fw2 = new FileWriter(thirdSampleFile);
        fw2.write("ijkl");
        fw2.close();
        emptyDirectory.mkdir();
    }

    /**
     * mergeFilesTest() will test the mergeFiles() method in Merger class
     * by passing a sample directory which contains files with some content in it
     * and checks if files in a directory are merged into single file
     * @throws IOException
     */
    @Test
    public void mergeFilesTest() throws IOException {
        Merger merger = new Merger();
        merger.mergeFiles(sampleDirectory.getPath());
        String str = new String(Files.readAllBytes(Paths.get(auditLogFile.getPath())));
        //System.out.println(str);
        String fileText = str.replace("\n", "");
        //System.out.println(fileText);
        assertTrue(fileText.contains("abcd"));
    }

    /**
     * mergeFilesTestEmpty() will test the mergeFiles()
     * by passing an empty directory and checks if merged file is created or not
     * @throws IOException
     */
    @Test
    public void mergeFilesTestEmpty() throws IOException {
        Merger merger = new Merger();
        merger.mergeFiles(emptyDirectory.getPath());
        boolean fileExists = auditLogFile.exists();
        //System.out.println(fileExists);
        assertTrue(fileExists);
        //assertFalse(fileExists);
    }

    /**
     * delete all the sample directories and files after each test
     * @throws Exception
     */
    @After
    public void tearDown() throws Exception {
        firstSampleFile.delete();
        secondSampleFile.delete();
        thirdSampleFile.delete();
        sampleDirectory.delete();
        emptyDirectory.delete();
    }

}