package com.example.kafka.config;

import com.example.avro.SimpleMessage;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Конфигурация Kafka для приложения:
 * - ProducerFactory и KafkaTemplate для отправки Avro сообщений (значение — SimpleMessage).
 * - ConsumerFactory и ConcurrentKafkaListenerContainerFactory для приёма сообщений через @KafkaListener.
 *
 * Используются сериализатор/десериализатор Confluent (KafkaAvroSerializer / KafkaAvroDeserializer)
 * и Schema Registry (schemaRegistryUrl) для управления схемами Avro.
 */
@Configuration
public class KafkaConfig {

    // Адрес(а) bootstrap-серверов Kafka, подставляется из application.properties / окружения
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // URL Schema Registry (Confluent) для сериализации/десериализации Avro
    @Value("${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    /**
     * ProducerFactory настраивает свойства продюсера:
     * - bootstrap servers
     * - сериализатор ключа (String)
     * - сериализатор значения (KafkaAvroSerializer) — работает вместе со Schema Registry
     *
     * Возвращает DefaultKafkaProducerFactory, который используется KafkaTemplate для отправки сообщений.
     */
    @Bean
    public ProducerFactory<String, SimpleMessage> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        // Адреса брокеров
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Класс сериализации ключа
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // Класс сериализации значения — Confluent Avro сериализатор
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        // Адрес Schema Registry (ключ строки совпадает с тем, что ожидает Confluent-сериализатор)
        props.put("schema.registry.url", schemaRegistryUrl);
        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * KafkaTemplate упрощает отправку сообщений (асинхронно/с колбэками).
     * Шаблон типизирован как <String, SimpleMessage>.
     */
    @Bean
    public KafkaTemplate<String, SimpleMessage> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    /**
     * ConsumerFactory настраивает потребителя:
     * - bootstrap servers
     * - десериализатор ключа (String)
     * - десериализатор значения (KafkaAvroDeserializer) — использует Schema Registry для получения схемы
     * - specific.avro.reader = true — десериализатор вернёт конкретный класс (Generated SpecificRecord),
     *   а не GenericRecord. Это позволяет получать SimpleMessage напрямую.
     *
     * Важно: specific.avro.reader принято задавать как boolean true, а не строкой "true".
     */
    @Bean
    public ConsumerFactory<String, SimpleMessage> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        // Адреса брокеров
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		// Десериализация ключа
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // Десериализация значения — Confluent Avro десериализатор
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        // URL Schema Registry (совместим с Confluent десериализатором)
        props.put("schema.registry.url", schemaRegistryUrl);
        // Возвращать SpecificRecord (сгенерированный класс SimpleMessage), а не GenericRecord
        props.put("specific.avro.reader", Boolean.TRUE);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Конфигурация фабрики контейнеров для @KafkaListener:
     * - задаём ConsumerFactory
     * - можно дополнительно настроить concurrency (количество потоков/парралельных consumer'ов),
     *   error handler, ack mode и т.д.
     *
     * По умолчанию используется авто-commit смещений, если в конфигурации не установлено иное.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, SimpleMessage> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, SimpleMessage> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        // При желании: factory.setConcurrency(3); // параллельные потоки для консьюмера
        return factory;
    }
}