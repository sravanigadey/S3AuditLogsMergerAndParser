package com.logs;

import org.apache.log4j.Logger;
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
public class TestMerger {

    private final Logger LOG = Logger.getLogger(TestMerger.class);

    private final Merger merger = new Merger();

    /**
     * sample directories and files to test
     */
    private final File auditLogFile = new File("AuditLogFile");
    private final File sampleDirectory = new File("sampleFilesDirectory");
    private final File emptyDirectory = new File("emptyFilesDirectory");
    private final File firstSampleFile = new File("sampleFilesDirectory","sampleFile1.txt");
    private final File secondSampleFile = new File("sampleFilesDirectory", "sampleFile2.txt");
    private final File thirdSampleFile = new File("sampleFilesDirectory", "sampleFile3.txt");

    /**
     * creates the sample directories and files before each test
     * @throws Exception
     */
    @Before
    public void setUp() throws IOException {
        sampleDirectory.mkdir();
        emptyDirectory.mkdir();
        try (FileWriter fw = new FileWriter(firstSampleFile);
             FileWriter fw1 = new FileWriter(secondSampleFile);
             FileWriter fw2 = new FileWriter(thirdSampleFile)) {
            fw.write("abcd");
            fw1.write("efgh");
            fw2.write("ijkl");
        }
    }

    /**
     * mergeFilesTest() will test the mergeFiles() method in Merger class
     * by passing a sample directory which contains files with some content in it
     * and checks if files in a directory are merged into single file
     * @throws IOException
     */
    @Test
    public void mergeFilesTest() throws IOException {
        merger.mergeFiles(sampleDirectory.getPath());
        String str = new String(Files.readAllBytes(Paths.get(auditLogFile.getPath())));
        String fileText = str.replace("\n", "");
        assertTrue("the string 'abcd' should be in the merged file", fileText.contains("abcd"));
        assertTrue("the string 'efgh' should be in the merged file", fileText.contains("efgh"));
        assertTrue("the string 'ijkl' should be in the merged file", fileText.contains("ijkl"));
    }

    /**
     * mergeFilesTestEmpty() will test the mergeFiles()
     * by passing an empty directory and checks if merged file is created or not
     * @throws IOException
     */
    @Test
    public void mergeFilesTestEmpty() throws IOException {
        if(auditLogFile.exists()) {
            LOG.info("AuditLogFile already exists and we are deleting it here");
            auditLogFile.delete();
        }
        merger.mergeFiles(emptyDirectory.getPath());
        assertFalse("This AuditLogFile shouldn't exist if input directory is empty ", auditLogFile.exists());
    }

    /**
     * delete all the sample directories and files after all tests
     * @throws Exception
     */
    @After
    public void tearDown() throws Exception {
        auditLogFile.delete();
        firstSampleFile.delete();
        secondSampleFile.delete();
        thirdSampleFile.delete();
        sampleDirectory.delete();
        emptyDirectory.delete();
    }

}