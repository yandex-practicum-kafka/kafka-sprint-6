package com.example.kafka.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Общие свойства приложения, инжектируемые через Spring @Value из application.properties / переменных окружения.
 * Содержит:
 * - топик Kafka для продюсера/консьюмера
 * - интервал планировщика (ms) для периодической отправки сообщений
 * - префикс, добавляемый к контенту генерируемых сообщений
 */
@Component
public class AppProperties {

    /**
     * Имя Kafka-топика, используемого продюсером и консьюмером.
     * Ожидается свойство spring.kafka.topic в конфигурации.
     */
    @Value("${spring.kafka.topic}")
    private String kafkaTopic;

    /**
     * Интервал в миллисекундах для аннотированного @Scheduled метода.
     * Ожидается свойство app.scheduler.interval-ms.
     */
    @Value("${app.scheduler.interval-ms}")
    private long schedulerIntervalMs;

    /**
     * Префикс, который будет добавляться к генерируемому содержимому сообщения.
     * Ожидается свойство app.message.prefix.
     */
    @Value("${app.message.prefix}")
    private String messagePrefix;

    // Геттеры для доступа к свойствам из других бинов

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public long getSchedulerIntervalMs() {
        return schedulerIntervalMs;
    }

    public String getMessagePrefix() {
        return messagePrefix;
    }
}