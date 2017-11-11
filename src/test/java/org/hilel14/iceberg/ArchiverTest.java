package org.hilel14.iceberg;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author hilel14
 */
public class ArchiverTest {

    public ArchiverTest() {
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

    //@Test
    public void testCreateArchive() throws Exception {
        String job = "job-1.json";
        Archiver instance = new Archiver(job);
        instance.createArchive();
    }

}
