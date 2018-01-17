package org.hilel14.iceberg;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Date;
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

    public String getVaultName() {
        // VaultARN example: "arn:aws:glacier:eu-west-1:243247906295:vaults/lenovo
        String[] parts = vaultArn.split(":");
        String last = parts[parts.length - 1];
        String name = last.replace("vaults/", "");
        return name;
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
