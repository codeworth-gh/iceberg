/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hilel14.iceberg;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author hilel14
 */
public class SnapshotTest {

    public SnapshotTest() {
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

    @Test
    public void testLoad() throws Exception {
        Path source = Paths.get("/tmp/snapshot.json");
        Snapshot instance = new Snapshot();
        instance.load(source);
        System.out.println(instance.getHashToPaths());
        System.out.println(instance.getFileHashes());
    }

    //@Test
    public void testSave() throws Exception {
        Path target = Paths.get("/tmp/snapshot.json");
        Snapshot instance = new Snapshot();
        Set<Path> paths = new HashSet<>();
        paths.add(Paths.get("/var/opt/data/1.txt"));
        paths.add(Paths.get("/var/opt/data/2.txt"));
        instance.getHashToPaths().put("h1", paths);
        paths.add(Paths.get("/var/opt/data/מספר שלוש.txt"));
        instance.getHashToPaths().put("h2", paths);
        instance.save(target);
    }

}
