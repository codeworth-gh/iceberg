package org.hilel14.iceberg.model;

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
