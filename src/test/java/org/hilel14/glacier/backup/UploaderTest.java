package org.hilel14.glacier.backup;

import java.util.regex.Pattern;
import org.junit.Assert;

/**
 *
 * @author hilel14
 */
public class UploaderTest {

    @org.junit.Test
    public void testRegEx() {
        String regex = ".+\\.(csv|doc|docx|jpeg)";
        Pattern pattern = Pattern.compile(regex);
        Assert.assertTrue(pattern.matcher("123.csv").matches());
        Assert.assertTrue(pattern.matcher("123.doc").matches());
        Assert.assertFalse(pattern.matcher("123.gif").matches());
        Assert.assertFalse(pattern.matcher("123.docxZ").matches());
        Assert.assertFalse(pattern.matcher(".gif").matches());
    }

}
