package com.facebook.presto.catalog;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * @author sunjiantao
 * @date 2020-12-17
 */
//@Data
@JsonSerialize
public class CatalogTest {
    private Integer id;
    private String catalogName;
    private String connectorName;
    private String creator;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public String getConnectorName() {
        return connectorName;
    }

    public void setConnectorName(String connectorName) {
        this.connectorName = connectorName;
    }

    public String getCreator() {
        return creator;
    }

    public void setCreator(String creator) {
        this.creator = creator;
    }
}
