package org.hilel14.iceberg;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;
import org.junit.Assert;

/**
 *
 * @author hilel14
 */
public class MiscTest {

    //@org.junit.Test
    public void testRegEx() {
        String regex = ".+\\.(csv|doc|docx|jpeg)";
        Pattern pattern = Pattern.compile(regex);
        Assert.assertTrue(pattern.matcher("123.csv").matches());
        Assert.assertTrue(pattern.matcher("123.doc").matches());
        Assert.assertFalse(pattern.matcher("123.gif").matches());
        Assert.assertFalse(pattern.matcher("123.docxZ").matches());
        Assert.assertFalse(pattern.matcher(".gif").matches());
    }

    //@org.junit.Test
    public void jsonTest() throws Exception {
        Path target = Paths.get("/tmp/test.json");
        try (
                FileOutputStream out = new FileOutputStream(target.toFile());
                JsonGenerator jsonGenerator = new JsonFactory().createGenerator(out)) {
            jsonGenerator.setPrettyPrinter(new DefaultPrettyPrinter());
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("description", "Iceberg snapshot");
            jsonGenerator.writeArrayFieldStart("phoneNumbers");
            jsonGenerator.writeNumber(1);
            jsonGenerator.writeNumber(2);
            jsonGenerator.writeNumber(3);
            jsonGenerator.writeEndArray();
            jsonGenerator.writeEndObject(); //closing root object
            jsonGenerator.flush();
        }
    }

}
