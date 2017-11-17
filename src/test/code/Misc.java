
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.utils.IOUtils;
import org.hilel14.iceberg.ArchiverTest;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author hilel14
 */
public class Misc {

    private void extractSampleFiles() throws Exception {
        InputStream in = ArchiverTest.class.getResourceAsStream("/data/in.zip");
        ArchiveStreamFactory factory = new ArchiveStreamFactory();
        try (ArchiveInputStream archiveInputStream
                = factory.createArchiveInputStream(ArchiveStreamFactory.ZIP, in)) {
            ZipArchiveEntry entry = (ZipArchiveEntry) archiveInputStream.getNextEntry();
            while (entry != null) {
                File outputFile = inFolder.resolve(entry.getName()).toFile();
                try (OutputStream outStream = new FileOutputStream(outputFile)) {
                    IOUtils.copy(archiveInputStream, outStream);
                }
                entry = (ZipArchiveEntry) archiveInputStream.getNextEntry();
            }
        }
    }
}
