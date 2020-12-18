/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.catalog;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * catalog与数据库交互
 *
 * @author sunjiantao
 * @date 2020-12-14
 */
public class CatalogDao
{
    public static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    public static final String JDBC_URL = "jdbc:mysql://localhost:3306/jiaodong?characterEncoding=utf8&useSSL=false&serverTimeZone=Asia/Shanghai";
    public static final String JDBC_USER = "root";
    public static final String JDBC_PASSWORD = "123456";

    public static void main(String[] args)
    {
        CatalogDao dao = new CatalogDao();
        CatalogInfo info = new CatalogInfo();
        Map<String, String> properties = new HashMap<>();
        properties.put("test", "test111");
        info.setCatalogName("testname");
        info.setConnectorName("testconndddec11t");
        info.setCreator("me");
        info.setProperties(properties);
        try {
            dao.save(info);
//            dao.getAll();
//            dao.delete(2);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void save(CatalogInfo catalogInfo) throws Exception
    {
        // JDBC连接的URL, 不同数据库有不同的格式:
        Class.forName(JDBC_DRIVER);
        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD)) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO catalog_info (catalog_name, connector_name, creator, properties) VALUES (?,?,?,?)")) {
                ps.setObject(1, catalogInfo.getCatalogName());
                ps.setObject(2, catalogInfo.getConnectorName());
                ps.setObject(3, catalogInfo.getCreator());
                ObjectMapper mapper = new ObjectMapper();
                ps.setObject(4, mapper.writeValueAsString(catalogInfo.getProperties()));
                int n = ps.executeUpdate();
            }
        }
    }

    public List<CatalogInfo> getAll() throws Exception
    {
        // JDBC连接的URL, 不同数据库有不同的格式:
        Class.forName(JDBC_DRIVER);
        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD)) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT id,catalog_name, connector_name, creator, properties FROM catalog_info")) {
                    List<CatalogInfo> catalogInfoList = new ArrayList<>();
                    while (rs.next()) {
                        Integer id = rs.getInt(1);
                        String catalogName = rs.getString(2); // 注意：索引从1开始
                        String connectorName = rs.getString(3);
                        String creator = rs.getString(4);
                        String properties = rs.getString(5);
                        ObjectMapper mapper = new ObjectMapper();
//                        Map<String, String> maps = JSON.parseObject(properties, Map.class);
                        Map<String, String> maps = mapper.readValue(properties, Map.class);
                        CatalogInfo info = new CatalogInfo();
                        info.setCatalogName(catalogName);
                        info.setConnectorName(connectorName);
                        info.setCreator(creator);
                        info.setProperties(maps);
                        info.setId(id);
                        catalogInfoList.add(info);
                    }
                    return catalogInfoList;
                }
            }
        }
    }

    public void delete(Integer id) throws Exception
    {
        Class.forName(JDBC_DRIVER);
        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD)) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "DELETE FROM catalog_info where id = ?")) {
                ps.setObject(1, id);
                int n = ps.executeUpdate();
            }
        }
    }
}
