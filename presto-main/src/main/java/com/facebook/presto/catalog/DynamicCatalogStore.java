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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.metadata.StaticCatalogStoreConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;

import javax.inject.Inject;
import java.io.File;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;

public class DynamicCatalogStore {
    private static final Logger log = Logger.get(DynamicCatalogStore.class);
    private final ConnectorManager connectorManager;
    private final File catalogConfigurationDir;
    private final Set<String> disabledCatalogs;
    private final AtomicBoolean catalogsLoading = new AtomicBoolean();
    private final AtomicBoolean catalogsLoaded = new AtomicBoolean();
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private final Map<String, CatalogInfo> catalogInfoCache = new HashMap<>();

    @Inject
    public DynamicCatalogStore(ConnectorManager connectorManager, StaticCatalogStoreConfig config) {
        this(connectorManager,
                config.getCatalogConfigurationDir(),
                firstNonNull(config.getDisabledCatalogs(), ImmutableList.of()));
    }

    public DynamicCatalogStore(ConnectorManager connectorManager, File catalogConfigurationDir, List<String> disabledCatalogs) {
        this.connectorManager = connectorManager;
        this.catalogConfigurationDir = catalogConfigurationDir;
        this.disabledCatalogs = ImmutableSet.copyOf(disabledCatalogs);
    }

    public boolean areCatalogsLoaded() {
        return catalogsLoaded.get();
    }

    public void loadCatalogs()
            throws Exception {
        loadCatalogs(ImmutableMap.of());
    }

    public void loadCatalogs(Map<String, Map<String, String>> additionalCatalogs)
            throws Exception {
        if (!catalogsLoading.compareAndSet(false, true)) {
            return;
        }
//        for (File file : listFiles(catalogConfigurationDir)) {
//            if (file.isFile() && file.getName().endsWith(".properties")) {
//                loadCatalog(file);
//            }
//        }
        //todo 修改为从mysql数据库加载方法。
        load();
        scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                reload();
            }
        }, 60, 60, TimeUnit.SECONDS);

        additionalCatalogs.forEach(this::loadCatalog);

        catalogsLoaded.set(true);
    }

    /**
     * 1、获取所有的cataloginfo
     * 2、执行loadcatalog方法
     * 3、cataloginfo添加到cataloginfo cache
     */
    private void load() {
        CatalogDao catalogDao = new CatalogDao();
        List<CatalogInfo> catalogInfoList = new ArrayList<>();
        try {
            catalogInfoList = catalogDao.getAll();
        } catch (Exception e) {
            e.printStackTrace();
        }
        catalogInfoList.forEach(catalogInfo -> {
            try {
                loadCatalog(catalogInfo);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        putIntoCache(catalogInfoList);
    }

    private void putIntoCache(List<CatalogInfo> info) {
        info.forEach(i -> {
            catalogInfoCache.put(i.getCatalogName(), i);
        });
    }

    /**
     * 1、获取最新的dcataloginfo
     * 2、与现有的cataloginfo cache对比
     * 3、对新增、修改、删除的cataloginfo操作。
     */
    private void reload() {
        CatalogDao catalogDao = new CatalogDao();
        List<CatalogInfo> newestCatInfo = new ArrayList<>();
        try {
            newestCatInfo = catalogDao.getAll();
        } catch (Exception e) {
            e.printStackTrace();
        }
        catalogInfoCache.clear();
        newestCatInfo.forEach(i -> {
            catalogInfoCache.put(i.getCatalogName(), i);
        });
    }

    private void loadCatalog(CatalogInfo catalogInfo)
            throws Exception {
        String catalogName = Files.getNameWithoutExtension(catalogInfo.getCatalogName());

        log.info("-- Loading catalog properties %s --", catalogInfo.getCatalogName());
        Map<String, String> properties = catalogInfo.getProperties();
        checkState(properties.containsKey("connector.name"), "Catalog configuration %s does not contain connector.name", catalogInfo.getConnectorName());
        loadCatalog(catalogName, properties);
    }

    private void loadCatalog(String catalogName, Map<String, String> properties) {
        if (disabledCatalogs.contains(catalogName)) {
            log.info("Skipping disabled catalog %s", catalogName);
            return;
        }

        log.info("-- Loading catalog %s --", catalogName);

        String connectorName = null;
        ImmutableMap.Builder<String, String> connectorProperties = ImmutableMap.builder();
        for (Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().equals("connector.name")) {
                connectorName = entry.getValue();
            } else {
                connectorProperties.put(entry.getKey(), entry.getValue());
            }
        }

        checkState(connectorName != null, "Configuration for catalog %s does not contain connector.name", catalogName);

        connectorManager.createConnection(catalogName, connectorName, connectorProperties.build());
        log.info("-- Added catalog %s using connector %s --", catalogName, connectorName);
    }

    private static List<File> listFiles(File installedPluginsDir) {
        if (installedPluginsDir != null && installedPluginsDir.isDirectory()) {
            File[] files = installedPluginsDir.listFiles();
            if (files != null) {
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }
}
