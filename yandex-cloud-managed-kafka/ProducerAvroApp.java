package com.example;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

// Производитель сообщений, ProducerAvroApp
// (переименуется из ProducerAvroApp.java в App.java в соответствующем каталоге при развёртывании)
public class App {
    public static void main(String[] args) throws InterruptedException {
        // Адреса серверов Kafka (bootstrap servers)
        final String BOOTSTRAP = "rc1a-6p0ipkofmb01gu4n.mdb.yandexcloud.net:9091,rc1b-pmc6kj097a9bv0ve.mdb.yandexcloud.net:9091,rc1d-kl3qb8ueq2995teu.mdb.yandexcloud.net:9091";
        // Имя Kafka топика, куда отправляются сообщения
        final String TOPIC = "test-topic";
        // Имя пользователя для аутентификации в Kafka
        final String USER = "test-user";
        // Пароль для аутентификации в Kafka
        final String PASS = "test-password";

        // Путь к хранилищу доверенных сертификатов SSL (truststore)
        final String SSL_TRUSTSTORE = "/etc/security/ssl";
        // Пароль для доступа к хранилищу доверенных сертификатов SSL
        final String SSL_TRUSTSTORE_PASS = "storepass";

        // Адреса серверов Schema Registry
        final String SCHEMA_REGISTRY_URL = "https://rc1a-6p0ipkofmb01gu4n.mdb.yandexcloud.net:443,https://rc1b-pmc6kj097a9bv0ve.mdb.yandexcloud.net:443,https://rc1d-kl3qb8ueq2995teu.mdb.yandexcloud.net:443";

        // Настройка JVM truststore для HTTP клиента Schema Registry (необходим для работы с Schema Registry по HTTPS)
        System.setProperty("javax.net.ssl.trustStore", SSL_TRUSTSTORE);
        System.setProperty("javax.net.ssl.trustStorePassword", SSL_TRUSTSTORE_PASS);

        // Определение Avro схемы для значения сообщения (value)
        String valueSchemaStr =
                "{ \"namespace\": \"my.test\", \"name\": \"value\", \"type\": \"record\", \"fields\": [ {\"name\": \"name\", \"type\": \"string\"} ] }";
        // Определение Avro схемы для ключа сообщения (key)
        String keySchemaStr =
                "{ \"namespace\": \"my.test\", \"name\": \"key\", \"type\": \"record\", \"fields\": [ {\"name\": \"name\", \"type\": \"string\"} ] }";

        // Создание парсера Avro схемы
        Schema.Parser parser = new Schema.Parser();
        // Парсинг Avro схемы для значения
        Schema valueSchema = parser.parse(valueSchemaStr);
        // Парсинг Avro схемы для ключа
        Schema keySchema = parser.parse(keySchemaStr);

        // Создание билдеров GenericRecord для ключа и значения.
        // GenericRecord - это представление Avro записи в памяти.
        GenericRecordBuilder valueBuilder = new GenericRecordBuilder(valueSchema);
        GenericRecordBuilder keyBuilder = new GenericRecordBuilder(keySchema);

        // ObjectMapper для преобразования GenericRecord в JSON строку (для логирования)
        ObjectMapper mapper = new ObjectMapper();

        // Создание Properties для конфигурации Kafka Producer
        Properties props = new Properties();
        // Установка адресов bootstrap серверов Kafka
        props.put("bootstrap.servers", BOOTSTRAP);
        // Установка режима подтверждения записи (acks=all - подтверждение от всех реплик)
        props.put("acks", "all");
        // Установка сериализатора для ключа (KafkaAvroSerializer - сериализация Avro)
        props.put("key.serializer", KafkaAvroSerializer.class.getName());
        // Установка сериализатора для значения (KafkaAvroSerializer - сериализация Avro)
        props.put("value.serializer", KafkaAvroSerializer.class.getName());

        // Настройка безопасности: SASL_SSL и SCRAM-SHA-512 для аутентификации
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "SCRAM-SHA-512");
        // Формирование строки JAAS для аутентификации
        String jaas = String.format("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                USER, PASS);
        props.put("sasl.jaas.config", jaas);

        // Настройка подключения к Schema Registry
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        // Указываем способ передачи учетных данных для Schema Registry (через USER_INFO)
        props.put("schema.registry.basic.auth.credentials.source", "USER_INFO");
        // Указываем учетные данные для Schema Registry
        props.put("schema.registry.basic.auth.user.info", USER + ":" + PASS);
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("basic.auth.user.info", USER + ":" + PASS);

        // Настройка truststore для SSL
        props.put("ssl.truststore.location", SSL_TRUSTSTORE);
        props.put("ssl.truststore.password", SSL_TRUSTSTORE_PASS);

        // Вывод URL Schema Registry и имени пользователя (замаскированного) в консоль для отладки
        System.out.println("SR url: " + props.getProperty("schema.registry.url"));
        String ui = props.getProperty("schema.registry.basic.auth.user.info", "");
        System.out.println("SR user info (masked): " + ui.replaceAll(":.+", ":***"));

        // Создание Kafka Producer с использованием настроек
        try (Producer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(props)) {
            long i = 1;
            // Бесконечный цикл отправки сообщений (ВНИМАНИЕ! Требует остановки вручную)
            while (true) {
                // Формирование значения имени для сообщения
                String valName = "Value-" + i;
                // Формирование значения имени для ключа сообщения
                String keyName = "Key-" + i;

                // Установка значения имени в builder для значения сообщения
                valueBuilder.set("name", valName);
                // Построение GenericRecord для значения сообщения
                GenericRecord valueRecord = valueBuilder.build();

                // Установка значения имени в builder для ключа сообщения
                keyBuilder.set("name", keyName);
                // Построение GenericRecord для ключа сообщения
                GenericRecord keyRecord = keyBuilder.build();

                // Создание ProducerRecord с ключом и значением
                ProducerRecord<GenericRecord, GenericRecord> record = new ProducerRecord<>(TOPIC, keyRecord, valueRecord);

                // Преобразование ключа и значения в JSON-строки для логирования (только для просмотра, не для передачи в Kafka)
                final String jsonKey = genericRecordToJson(mapper, keyRecord);
                final String jsonValue = genericRecordToJson(mapper, valueRecord);

                // Отправка сообщения в Kafka асинхронно с использованием Callback
                producer.send(record, (RecordMetadata metadata, Exception exception) -> {
                    // Callback, вызываемый после отправки сообщения
                    if (exception != null) {
                        // Обработка ошибки отправки
                        System.err.println("Delivery failed: " + exception);
                    } else {
                        // Обработка успешной отправки
                        System.out.println("Delivered message:");
                        System.out.println("  topic-partition-offset: " + metadata.topic() + "[" + metadata.partition() + "]@" + metadata.offset());
                        System.out.println("  key:   " + jsonKey);
                        System.out.println("  value: " + jsonValue);
                    }
                });

                // Увеличение счетчика
                i++;
                // Пауза в 1 секунду между отправками сообщений (для наглядности, в реальных системах лучше использовать batching)
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            // Обработка ошибок, возникших во время работы продюсера
            System.err.println("Producer failed: ");
            e.printStackTrace();
        }
    }

    /**
     * Преобразует {@link GenericRecord} (Avro) в JSON-строку для целей логирования и отладки.
     * Создает JSON объект, где каждое поле Avro записи становится полем JSON объекта.
     * Если значение поля Avro равно null, в JSON будет записано null.
     *
     * @param mapper {@link ObjectMapper} для выполнения преобразования в JSON.
     * @param rec {@link GenericRecord} для преобразования. Может быть null.
     * @return JSON-строку, представляющую {@link GenericRecord}. В случае ошибки преобразования, возвращает "{}".
     */
	private static String genericRecordToJson(ObjectMapper mapper, GenericRecord rec) {
        try {
            ObjectNode node = mapper.createObjectNode();
            if (rec != null) {
                for (Schema.Field f : rec.getSchema().getFields()) {
                    Object v = rec.get(f.name());
                    if (v == null) node.putNull(f.name());
                    else node.put(f.name(), v.toString());
                }
            }
            return mapper.writeValueAsString(node);
        } catch (Exception e) {
            return "{}";
        }
    }
}
