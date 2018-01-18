package org.hilel14.iceberg;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author hilel14
 */
public class Inventory {

    @JsonProperty("ArchiveList")
    private List<Map> archiveList;

    @JsonProperty("InventoryDate")
    private Date inventoryDate;

    @JsonProperty("VaultARN")
    private String vaultArn;

    /**
     * Extract information from AWS vault
     * <a href=https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html>ARN</a>.
     * Example: arn:aws:glacier:eu-west-1:243247906295:vaults/lenovo
     *
     * @return A map containing AWS account, region and vault name.
     */
    public Map<String, String> extractVaultArn() {

        Map<String, String> map = new HashMap<>();
        String[] parts = vaultArn.split(":");
        map.put("region", parts[3]);
        map.put("account", parts[4]);
        map.put("vault", parts[5].replace("vaults/", ""));
        return map;
    }

    /**
     * @return the archiveList
     */
    public List<Map> getArchiveList() {
        return archiveList;
    }

    /**
     * @param archiveList the archiveList to set
     */
    public void setArchiveList(List<Map> archiveList) {
        this.archiveList = archiveList;
    }

    /**
     * @return the inventoryDate
     */
    public Date getInventoryDate() {
        return inventoryDate;
    }

    /**
     * @param inventoryDate the inventoryDate to set
     */
    public void setInventoryDate(Date inventoryDate) {
        this.inventoryDate = inventoryDate;
    }

    /**
     * @return the vaultArn
     */
    public String getVaultArn() {
        return vaultArn;
    }

    /**
     * @param vaultArn the vaultArn to set
     */
    public void setVaultArn(String vaultArn) {
        this.vaultArn = vaultArn;
    }

}
