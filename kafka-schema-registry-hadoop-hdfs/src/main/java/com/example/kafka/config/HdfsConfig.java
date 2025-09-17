package com.example.kafka.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;

import java.net.URI;

/**
 * Конфигурация для создания Hadoop FileSystem бина, который будет инжектироваться в сервисы приложения.
 *
 * Поведение:
 * - Создаёт org.apache.hadoop.fs.FileSystem на основе URI и дополнительных свойств из HdfsProperties.
 * - Bean помечен destroyMethod="close" — при завершении контекста Spring FileSystem будет корректно закрыт.
 */
@org.springframework.context.annotation.Configuration
public class HdfsConfig {

    // Свойства (URI, user и т.д.) инжектируются из HdfsProperties
    @Autowired
    private HdfsProperties hdfsProperties;

    /**
     * Создаёт и возвращает бин FileSystem для доступа к HDFS.
     * destroyMethod="close" гарантирует вызов fileSystem.close() при остановке приложения.
     *
     * @return FileSystem, подключённый к указанному fs.defaultFS
     * @throws Exception при ошибках создания FileSystem (например неверный URI или проблемы с аутентификацией)
     */
    @Bean(destroyMethod = "close")
    public FileSystem hdfsFileSystem() throws Exception {
        // Конфигурация Hadoop клиента
        Configuration conf = new Configuration();

        // Устанавливаем fs.defaultFS (например hdfs://namenode:9000)
        conf.set("fs.defaultFS", hdfsProperties.getUri());

        // Управление использованием hostname для DataNode (dfs.client.use.datanode.hostname)
        conf.set("dfs.client.use.datanode.hostname", String.valueOf(hdfsProperties.isUseDatanodeHostname()));

        // Пользователь, от имени которого будет создан FileSystem (можно задать через HdfsProperties)
        String user = hdfsProperties.getUser();

        // Получаем FileSystem для указанного URI с данной конфигурацией и пользователем.
        // Важно: при Kerberos-авторизации может потребоваться предварительный login.
        return FileSystem.get(new URI(hdfsProperties.getUri()), conf, user);
    }
}
