package com.facebook.presto.catalog;

import java.io.Serializable;
import java.util.Map;

/**
 * @author sunjiantao
 * @date 2020-12-14
 */
public class CatalogInfo implements Serializable {
    public Integer id;
    public String catalogName;
    public String connectorName;
    public String creator;
    public Map<String, String> properties;
    private static final long serialVersionUID = 1L;

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

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }
}
