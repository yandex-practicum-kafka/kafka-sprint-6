package com.example.kafka.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Свойства конфигурации для взаимодействия с HDFS.
 * Значения подставляются из application.properties / окружения.
 *
 * Поддерживаются fallback'ы на переменные окружения (например HADOOP_FS_DEFAULTFS, HDFS_DIR, HADOOP_USER_NAME).
 */
@Component
public class HdfsProperties {

    /**
     * URI HDFS (fs.defaultFS). По умолчанию берётся из:
     * - принципа: app.hdfs.uri
     * - если не задано — из переменной окружения HADOOP_FS_DEFAULTFS
     * - если и она не задана — значение по умолчанию "hdfs://namenode:9000"
     */
    @Value("${app.hdfs.uri:${HADOOP_FS_DEFAULTFS:hdfs://namenode:9000}}")
    private String uri;

    /**
     * Базовая директория в HDFS, куда записываются файлы.
     * По умолчанию: /data (можно задать через app.hdfs.dir или переменную окружения HDFS_DIR).
     */
    @Value("${app.hdfs.dir:${HDFS_DIR:/data}}")
    private String dir;

    /**
     * Пользователь для доступа к HDFS (Hadoop user).
     * По умолчанию: "root", можно переопределить через app.hdfs.user или HADOOP_USER_NAME.
     */
    @Value("${app.hdfs.user:${HADOOP_USER_NAME:root}}")
    private String user;

    /**
     * Флаг использования hostnames у DataNode при подключении клиента.
     * Отвечает за параметр dfs.client.use.datanode.hostname.
     * По умолчанию: true. Можно задать через app.hdfs.use-datanode-hostname или HDFS_CLIENT_USE_DATANODE_HOSTNAME.
     */
    @Value("${app.hdfs.use-datanode-hostname:${HDFS_CLIENT_USE_DATANODE_HOSTNAME:true}}")
    private boolean useDatanodeHostname = true;

    /**
     * Префикс имени файлов, которые создаются в HDFS.
     * По умолчанию: "simple-messages-"
     */
    @Value("${app.hdfs.file-prefix:simple-messages-}")
    private String filePrefix = "simple-messages-";

    /**
     * Расширение файлов, создаваемых в HDFS (например .ndjson).
     * По умолчанию: ".ndjson"
     */
    @Value("${app.hdfs.file-extension:.ndjson}")
    private String fileExtension = ".ndjson";

    // Геттеры для доступа к свойствам из других бинов

    public String getUri() {
        return uri;
    }

    public String getDir() {
        return dir;
    }

    public String getUser() {
        return user;
    }

    public boolean isUseDatanodeHostname() {
        return useDatanodeHostname;
    }

    public String getFilePrefix() {
        return filePrefix;
    }

    public String getFileExtension() {
        return fileExtension;
    }
}