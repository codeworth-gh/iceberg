package org.hilel14.iceberg;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author hilel14
 */
public class ArchiveCreatorTest {

    public ArchiveCreatorTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of createArchive method, of class ArchiveCreator.
     */
    @Test
    public void testCreateArchive() throws Exception {
        System.out.println("createArchive");
        String job = "job-1.json";
        ArchiveCreator instance = new ArchiveCreator(job);
        instance.createArchive();
    }

}
