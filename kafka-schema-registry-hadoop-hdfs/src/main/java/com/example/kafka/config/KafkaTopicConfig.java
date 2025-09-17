package com.example.kafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * Конфигурация для автоматического создания Kafka-топика при старте приложения.
 * - Создаёт KafkaAdmin, который использует AdminClient для управления кластером.
 * - Регистрирует бин NewTopic — Spring попытается создать топик с указанными параметрами.
 */
@Configuration
public class KafkaTopicConfig {

    // Адрес(а) bootstrap-сервера Kafka (например kafka-1:9092,kafka-2:9092,...)
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // Имя топика, по умолчанию "test-topic" если не задано в конфигурации
    @Value("${spring.kafka.topic:test-topic}")
    private String topicName;

    /**
     * Bean KafkaAdmin нужен для работы административных операций с Kafka (создание тем и т.д.).
     * Конфигурация передаётся в AdminClient через ключ AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG.
     * При наличии этого бина Spring автоматически попытается создать бины типа NewTopic.
     */
    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(configs);
    }

    /**
     * Bean NewTopic описывает топик, который нужно создать:
     * - имя берётся из настроек
     * - partitions = 3 (количество партиций)
     * - replicas = 3 (коэффициент репликации)
     *
     * Важно: replication-factor не может быть больше числа доступных брокеров.
     * В окружениях с меньшим количеством брокеров создание топика с replicas=3 завершится с ошибкой.
     */
    @Bean
    public NewTopic topic() {
        return TopicBuilder.name(topicName)
                .partitions(3)
                .replicas(3)
                .build();
    }
}