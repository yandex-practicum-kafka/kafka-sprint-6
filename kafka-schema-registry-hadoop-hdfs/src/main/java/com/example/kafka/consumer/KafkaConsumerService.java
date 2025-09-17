package com.example.kafka.consumer;

import com.example.avro.SimpleMessage;
import com.example.kafka.hdfs.HdfsService;
import org.springframework.context.annotation.Profile;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.nio.charset.StandardCharsets;

/**
 * Сервис-консьюмер, который слушает Kafka-топик и записывает принятые сообщения в HDFS.
 * Работает только в Spring-профиле "consumer".
 *
 * Поведение:
 * - При получении записи извлекает ключ, значение (Avro SimpleMessage), partition, offset и заголовки.
 * - Ставит id сообщения в MDC для удобной корреляции логов.
 * - Делегирует запись в HDFS в HdfsService.writeMessageUniqueFile().
 * - Логирует успешную обработку или ошибку. В текущей реализации при ошибке запись не переотправляется автоматически —
 *   опционально можно бросать RuntimeException для триггера ретраев или реализовать DLQ.
 */
@Service
@Profile("consumer")
@RequiredArgsConstructor
@Slf4j
public class KafkaConsumerService {

    // Сервис для записи сообщений в HDFS (инжектируется Spring'ом)
    private final HdfsService hdfsService;

    /**
     * Метод подписки на Kafka-топик.
     * Значения topics и groupId берутся из application.properties/yml:
     * spring.kafka.topic и spring.kafka.consumer.group-id
     *
     * @param record полученная запись Kafka (ключ String, значение SimpleMessage)
     */
    @KafkaListener(topics = "${spring.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeMessage(ConsumerRecord<String, SimpleMessage> record) {
        // Извлекаем данные сообщения
        SimpleMessage message = record.value();
        String key = record.key();
        int partition = record.partition();
        long offset = record.offset();
        long kafkaTimestamp = record.timestamp();
        Headers headers = record.headers();

        // Попытка получить id для корелляции логов (если SimpleMessage содержит getId())
        String messageId = null;
        try {
            messageId = message != null ? message.getId() : null;
        } catch (Exception ex) {
            // Если геттер отсутствует или другая структура — просто игнорируем и продолжаем
        }

        // Помещаем messageId в MDC для того, чтобы все последующие логи содержали его
        if (messageId != null) {
            MDC.put("messageId", messageId);
        }

        log.debug("Received record. topic={}, partition={}, offset={}, key={}, kafkaTs={} headers={}",
                record.topic(), partition, offset, key, kafkaTimestamp, headers);

        Instant start = Instant.now();
        try {
            log.info("Consuming message id={} key={} partition={} offset={}", messageId, key, partition, offset);

            // Запись сообщения в HDFS. Подробное логирование внутри HdfsService.
            hdfsService.writeMessageUniqueFile(message.toString());

            // Логирование метрик обработки: время и размер полезной нагрузки
            Instant end = Instant.now();
            long durationMs = Duration.between(start, end).toMillis();
            int payloadBytes = message.toString().getBytes(StandardCharsets.UTF_8).length;
            log.info("Processed message id={} partition={} offset={} durationMs={} payloadBytes={}",
                    messageId, partition, offset, durationMs, payloadBytes);
            log.debug("Full message payload: {}", message);
			
			// Внимание: здесь предполагается авто-commit смещений (auto-ack).
            // Если используется ручная подтверждения (AckMode.MANUAL), нужно вызвать ack.acknowledge() здесь.

        } catch (Exception e) {
            long durationMs = Duration.between(start, Instant.now()).toMillis();
            log.error("Failed to process message id={} partition={} offset={} after {} ms. Error: {}",
                    messageId, partition, offset, durationMs, e.toString());
            log.debug("Write error stacktrace:", e);

            // Возможные стратегии обработки ошибок:

            // 1) Игнорировать и продолжить (текущее поведение) — сообщение может быть потеряно.

            // 2) Бросить RuntimeException -> Kafka клиент попытается выполнить retry согласно конфигу.
            //    throw new RuntimeException(e);

            // 3) Отправить сообщение в DLQ (dead-letter queue) для последующей ручной обработки (рекомендуется в продуктиве).

        } finally {
            // Убираем messageId из MDC, чтобы не "протекал" в другие логи текущего потока
            if (messageId != null) {
                MDC.remove("messageId");
            }
        }
    }
}