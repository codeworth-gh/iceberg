package org.hilel14.iceberg;

import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author hilel14
 */
public class Uploader {

    static final Logger LOGGER = Logger.getLogger(Uploader.class.getName());

    public void uploadArchive(Path archive) {
        LOGGER.log(Level.INFO, "uploading {0}", archive);
    }
}
