package org.hilel14.iceberg;

import org.hilel14.iceberg.model.Job;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 * @author hilel14
 */
public class Configuration {

    static final Logger LOGGER = Logger.getLogger(Configuration.class.getName());
    private final Path workFolder;
    private final Map<String, Job> backupJobs = new HashMap<>();

    public Configuration() throws Exception {
        InputStream in = Configuration.class.getResourceAsStream("/iceberg.conf.xml");
        Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(in);
        XPath xpath = XPathFactory.newInstance().newXPath();
        // parse global properties
        workFolder = Paths.get(xpath.evaluate("conf/workFolder", doc));
        // parse backup jobs
        NodeList nodes = (NodeList) xpath.evaluate("conf/backupJobs/job", doc, XPathConstants.NODESET);
        Node node;
        for (int i = 0; i < nodes.getLength(); i++) {
            Job job = new Job();
            node = nodes.item(i);
            job.setId(xpath.evaluate("@id", node));
            job.setDescription(xpath.evaluate("description/text()", node));
            job.setSourceFolder(Paths.get(xpath.evaluate("source/path/text()", node)));
            job.setExcludeFilter(Pattern.compile(xpath.evaluate("source/excludePattern/text()", node)));
            job.setTargetEnabled(Boolean.parseBoolean(xpath.evaluate("target/@enabled", node)));
            job.setGlacierRegion(xpath.evaluate("target/region/text()", node));
            job.setGlacierVault(xpath.evaluate("target/vault/text()", node));
            backupJobs.put(job.getId(), job);
        }
        LOGGER.log(Level.INFO, "{0} jobs found", backupJobs.size());
    }

    /**
     * @return the workFolder
     */
    public Path getWorkFolder() {
        return workFolder;
    }

    /**
     * @return the backupJobs
     */
    public Map<String, Job> getBackupJobs() {
        return backupJobs;
    }

}
