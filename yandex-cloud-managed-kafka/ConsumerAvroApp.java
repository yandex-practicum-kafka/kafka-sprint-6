package com.example;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

// Потребитель сообщений, ConsumerAvroApp
// (переименуется из ConsumerAvroApp.java в App.java в соответствующем каталоге при развёртывании)
public class App {
    public static void main(String[] args) {

        // Адреса серверов Kafka (bootstrap servers)
        final String BOOTSTRAP = "rc1a-6p0ipkofmb01gu4n.mdb.yandexcloud.net:9091,rc1b-pmc6kj097a9bv0ve.mdb.yandexcloud.net:9091,rc1d-kl3qb8ueq2995teu.mdb.yandexcloud.net:9091";
        // Имя Kafka топика, из которого читаются сообщения
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

        // Настройка JVM truststore для Schema Registry HTTP клиента (необходим для работы с Schema Registry по HTTPS)
        System.setProperty("javax.net.ssl.trustStore", SSL_TRUSTSTORE);
        System.setProperty("javax.net.ssl.trustStorePassword", SSL_TRUSTSTORE_PASS);

        // Создание Properties для конфигурации Kafka Consumer
        Properties props = new Properties();
        // Установка адресов bootstrap серверов Kafka
        props.put("bootstrap.servers", BOOTSTRAP);
        // Установка ID группы потребителей
        props.put("group.id", "test-consumer-group");
        // Отключение автоматической фиксации смещения (offset)
        props.put("enable.auto.commit", "false");

        // Установка политики сброса смещения (offset) при отсутствии сохраненных коммитов
        // "latest" - начинаем с последних сообщений, "earliest" - с самых старых
        props.put("auto.offset.reset", "latest");

        // Читаем сырые байты — чтобы poll не падал на ошибке десериализации
        props.put("key.deserializer", ByteArrayDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());

        // Настройка подключения к Schema Registry и аутентификация
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
        props.put("schema.registry.ssl.truststore.location", SSL_TRUSTSTORE);
        props.put("schema.registry.ssl.truststore.password", SSL_TRUSTSTORE_PASS);

        // Настройка безопасности: SASL_SSL и SCRAM-SHA-512 для аутентификации
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "SCRAM-SHA-512");
        // Формирование строки JAAS для аутентификации
        String jaas = String.format("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";", USER, PASS);
        props.put("sasl.jaas.config", jaas);

        // Вывод URL Schema Registry и имени пользователя (замаскированного) в консоль для отладки
        System.out.println("SR url: " + props.getProperty("schema.registry.url"));
        String ui = props.getProperty("schema.registry.basic.auth.user.info", "");
        System.out.println("SR user info (masked): " + ui.replaceAll(":.+", ":***"));

        // Создание ObjectMapper для преобразования объектов в JSON строку
        ObjectMapper mapper = new ObjectMapper();

        // Avro deserializer используем вручную (чтобы попытаться распарсить только записанные Avro сообщения)
        KafkaAvroDeserializer avroDeserializer = new KafkaAvroDeserializer();
        // Конфигурация для Avro deserializer
        Map<String, Object> srConfig = new HashMap<>();
        srConfig.put("schema.registry.url", props.getProperty("schema.registry.url"));
        srConfig.put("schema.registry.basic.auth.credentials.source", props.getProperty("schema.registry.basic.auth.credentials.source"));
        srConfig.put("schema.registry.basic.auth.user.info", props.getProperty("schema.registry.basic.auth.user.info"));
        srConfig.put("basic.auth.credentials.source", props.getProperty("basic.auth.credentials.source"));
        srConfig.put("basic.auth.user.info", props.getProperty("basic.auth.user.info"));
        srConfig.put("schema.registry.ssl.truststore.location", props.getProperty("schema.registry.ssl.truststore.location"));
        srConfig.put("schema.registry.ssl.truststore.password", props.getProperty("schema.registry.ssl.truststore.password"));
        srConfig.put("specific.avro.reader", "false"); // Не требуем specific Avro reader
        avroDeserializer.configure(srConfig, false); // Конфигурируем десериализатор без привязки к ключу

        // Создание Kafka Consumer
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);

        // Rebance listener: при назначении переходить к концу (seekToEnd) — чтобы не читать старые несовместимые записи
        consumer.subscribe(Collections.singletonList(TOPIC), new ConsumerRebalanceListener() {
            // Метод, вызываемый при отзыве партиций
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) { /* noop */ }

            // Метод, вызываемый при назначении партиций
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // Перемещаемся в конец назначенных партиций (чтобы не читать с 0)
                try {
                    if (!partitions.isEmpty()) {
                        consumer.seekToEnd(partitions);
                        System.out.println("Seeked to end for assigned partitions: " + partitions);
                    }
                } catch (Exception e) {
                    System.err.println("seekToEnd failed: " + e.getMessage());
                }
            }
        });

        // Добавление shutdown hook для корректного завершения работы consumer
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { consumer.wakeup(); } catch (Exception ignored) {}
        }));

        try {
            System.out.println("Subscribed to topic " + TOPIC + ", polling...");
            // Основной цикл обработки сообщений
            while (true) {
                // Опрос Kafka на предмет новых сообщений (с таймаутом 1 секунда)
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(1));
                // Итерация по полученным сообщениям
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    // Инициализация переменных для представления ключа и значения в виде строк
                    String keyPrintable = "<null>";
                    String valuePrintable = "<null>";

                    // Получение сырых байтов ключа и значения
                    byte[] keyBytes = record.key();
                    byte[] valueBytes = record.value();

                    // Обработка ключа
                    if (keyBytes != null) {
                        try {
                            // Попытка десериализации ключа как Avro объекта
                            Object keyObj = avroDeserializer.deserialize(record.topic(), keyBytes);
                            // Преобразование десериализованного объекта в JSON строку
                            keyPrintable = objectToJson(mapper, keyObj);
                        } catch (Exception ex) {
                            // В случае ошибки десериализации, преобразуем сырые байты в JSON строку
                            keyPrintable = rawBytesToJson(mapper, keyBytes);
                            // Вывод сообщения об ошибке десериализации
                            System.err.println("Key deserialization failed at " + record.topic() + "[" + record.partition() + "]@" + record.offset() + ": " + ex.getMessage());
                        }
                    }

                    // Обработка значения
                    if (valueBytes != null) {
                        try {
                            // Попытка десериализации значения как Avro объекта
                            Object valObj = avroDeserializer.deserialize(record.topic(), valueBytes);
                            // Преобразование десериализованного объекта в JSON строку
                            valuePrintable = objectToJson(mapper, valObj);
                        } catch (Exception ex) {
                            // В случае ошибки десериализации, преобразуем сырые байты в JSON строку
                            valuePrintable = rawBytesToJson(mapper, valueBytes);
                            // Вывод сообщения об ошибке десериализации
                            System.err.println("Value deserialization failed at " + record.topic() + "[" + record.partition() + "]@" + record.offset() + ": " + ex.getMessage());
                        }
                    }

                    // Вывод информации о полученном сообщении
                    System.out.println("Consumed from " + record.topic() + " [" + record.partition() + "]@" + record.offset());
                    System.out.println("  key:   " + keyPrintable);
                    System.out.println("  value: " + valuePrintable);
                }

                // Коммит смещения (offset) после обработки каждой пачки сообщений
                if (!records.isEmpty()) {
                    try {
                        // Синхронный коммит смещения
                        consumer.commitSync();
                        System.out.println("Offsets committed");
                    } catch (Exception e) {
                        // Обработка ошибки коммита
                        System.err.println("Commit failed: " + e.getMessage());
                    }
                }
            }
        } catch (org.apache.kafka.common.errors.WakeupException we) {
            // нормальное завершение при вызове wakeup()
        } catch (Exception e) {
            // Обработка общих ошибок
            System.err.println("Consumer error: " + e.getMessage());
            e.printStackTrace(System.err);
        } finally {
            // Закрытие consumer в блоке finally для гарантии высвобождения ресурсов
            try { consumer.close(); } catch (Exception ignored) {}
            System.out.println("Consumer closed");
        }
    }

    /**
     * Преобразует произвольный объект в JSON строку. Поддерживает обработку {@link GenericRecord} (Avro),
     * а также простых типов, таких как String и Integer.
     * @param mapper {@link ObjectMapper} для выполнения преобразования в JSON.
     * @param obj Объект для преобразования. Может быть null, {@link GenericRecord} или объект простого типа.
     * @return JSON представление объекта. Если объект null, возвращает "null". Если преобразование завершается с ошибкой,
     *         возвращает строку "<json-conversion-error>".
     */
    private static String objectToJson(ObjectMapper mapper, Object obj) {
        try {
            if (obj == null) return "null";
            if (obj instanceof GenericRecord) {
                // Обработка GenericRecord (Avro)
                GenericRecord rec = (GenericRecord) obj;
                ObjectNode node = mapper.createObjectNode();
                for (org.apache.avro.Schema.Field f : rec.getSchema().getFields()) {
                    Object v = rec.get(f.name());
                    if (v == null) node.putNull(f.name());
                    else node.put(f.name(), v.toString());
                }
                return mapper.writeValueAsString(node);
            } else {
                // Для простых типов (String, Integer и т.д.) вернём JSON-строку/значение
                return mapper.writeValueAsString(obj);
            }
        } catch (Exception e) {
            // В случае ошибки преобразования, возвращаем строку с указанием ошибки
            return "\"<json-conversion-error>\"";
        }
    }

    /**
     * Преобразует массив сырых байтов в JSON строку.  Пытается интерпретировать байты как UTF-8 строку.
     * Если содержимое содержит непечатаемые байты, возвращает шестнадцатеричное представление.
     * @param mapper {@link ObjectMapper} для выполнения преобразования в JSON.
     * @param bytes Массив байтов для преобразования.
     * @return JSON представление массива байтов. Если байты представляют собой валидную UTF-8 строку,
     *         возвращается JSON представление этой строки.  Если байты содержат непечатаемые символы,
     *         возвращается JSON представление шестнадцатеричного представления этих байтов.
     *         В случае ошибки, возвращает строку "<binary>".
     */
	 private static String rawBytesToJson(ObjectMapper mapper, byte[] bytes) {
        try {
            // Попытка интерпретировать байты как UTF-8 строку
            String s = new String(bytes, StandardCharsets.UTF_8);
            // Если содержимое содержит непечатаемые байты, вернём hex представление
            boolean hasControl = s.chars().anyMatch(ch -> Character.isISOControl(ch) && ch != '\n' && ch != '\r' && ch != '\t');
            if (hasControl) {
                // вернуть hex как JSON строку
                return mapper.writeValueAsString(bytesToHex(bytes));
            } else {
                // корректная UTF-8 строка — вернуть как JSON строку
                return mapper.writeValueAsString(s);
            }
        } catch (Exception e) {
            // В случае ошибки, возвращаем строку с указанием, что это бинарные данные
            return "\"<binary>\"";
        }
    }

    /**
     * Преобразует массив байтов в его шестнадцатеричное представление в виде строки.
     * @param bytes Массив байтов для преобразования.
     * @return Строка, представляющая шестнадцатеричный вид массива байтов, с префиксом "0x".
     */
	 private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return "0x" + sb.toString();
    }
}
