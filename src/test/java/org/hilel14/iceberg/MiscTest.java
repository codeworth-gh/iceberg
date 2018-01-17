package org.hilel14.iceberg;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.regex.Pattern;
import org.junit.Assert;

/**
 *
 * @author hilel14
 */
public class MiscTest {

    //@org.junit.Test
    public void testRegEx() {
        // suffix
        String regex = ".+\\.(csv|doc)";
        Pattern pattern = Pattern.compile(regex);
        Assert.assertTrue(pattern.matcher("a.csv").matches());
        Assert.assertTrue(pattern.matcher("b.doc").matches());
        Assert.assertFalse(pattern.matcher("c.gif").matches());
        // prefix
        regex = "\\..+";
        pattern = Pattern.compile(regex);
        Assert.assertTrue(pattern.matcher(".a.txt").matches());
        Assert.assertFalse(pattern.matcher("b.txt").matches());
        // both
        regex = "\\..+|.+\\.(csv|doc)";
        pattern = Pattern.compile(regex);
        Assert.assertTrue(pattern.matcher("a.csv").matches());
        Assert.assertTrue(pattern.matcher("b.doc").matches());
        Assert.assertTrue(pattern.matcher(".c.xml").matches());
        Assert.assertFalse(pattern.matcher("c.xml").matches());
    }

    //@org.junit.Test
    public void generateLargFile() throws Exception {
        byte[] data = new byte[1024];
        try (OutputStream out = new FileOutputStream("/var/opt/data/files/largefile")) {
            for (int i = 0; i < 1000000; i++) {
                out.write(data);
            }
        }
    }

    //@org.junit.Test
    public void testCreationTimeFormat() {
        String source = "2017-12-08T11:59:02Z";
        String target = source.replaceAll(":", "-");
        Assert.assertEquals("2017-12-08T11-59-02Z", target);
    }

    @org.junit.Test
    public void testExtractVaultName() {
        String vaultArn = "arn:aws:glacier:eu-west-1:243247906295:vaults/lenovo";
        String[] parts = vaultArn.split(":");
        String last = parts[parts.length - 1];
        String vault = last.replace("vaults/", "");
        Assert.assertEquals("lenovo", vault);
    }
}
