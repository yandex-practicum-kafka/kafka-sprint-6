package com.example.kafka.producer;

import com.example.kafka.config.AppProperties;
import com.example.avro.SimpleMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * Сервис продюсера сообщений в Kafka.
 * Работает только в Spring-профиле "producer" (аннотация @Profile).
 * Примерно каждые app.scheduler.interval-ms миллисекунд формирует и отправляет Avro-сообщение SimpleMessage.
 */
@Service
@Profile("producer")
@RequiredArgsConstructor
@Slf4j
public class KafkaProducerService {

    // Шаблон Spring Kafka для отправки сообщений: ключ типа String, значение типа SimpleMessage (Avro)
    private final KafkaTemplate<String, SimpleMessage> kafkaTemplate;

    // Конфигурационные свойства приложения (тема, префикс сообщения, интервал и т.д.)
    private final AppProperties appProperties;

    /**
     * Планируемая задача, которая периодически отправляет сообщения в Kafka.
     * Значение fixedDelayString подставляется из application.properties/yml:
     * app.scheduler.interval-ms (или соответствующей переменной окружения).
     */
    @Scheduled(fixedDelayString = "${app.scheduler.interval-ms}")
    public void sendMessage() {
        // Генерация уникального id и контента сообщения
        String id = UUID.randomUUID().toString();
        String content = appProperties.getMessagePrefix() + UUID.randomUUID().toString();
        long ts = System.currentTimeMillis();

        // Создание Avro-сообщения через builder, соответствующее SimpleMessage.avsc
        SimpleMessage message = SimpleMessage.newBuilder()
                .setId(id)
                .setContent(content)
                .setTimestamp(ts)
                .build();

        // Топик берётся из настроек приложения
        String topic = appProperties.getKafkaTopic();

        // Оценка размера полезной нагрузки (в байтах) для логирования
        int payloadBytes = message.toString().getBytes(StandardCharsets.UTF_8).length;

        log.debug("Preparing to produce message. topic={}, id={}, payloadBytes={}, timestamp={}",
                topic, id, payloadBytes, ts);

        // Засекаем начало для измерения задержки отправки
        Instant start = Instant.now();
        try {
            // Асинхронная отправка сообщения: ключ = id, значение = Avro объект
            ListenableFuture<SendResult<String, SimpleMessage>> future =
                    kafkaTemplate.send(topic, id, message);

            // Подписываемся на результат отправки (успех/ошибка)
            future.addCallback(new ListenableFutureCallback<SendResult<String, SimpleMessage>>() {
                @Override
                public void onSuccess(SendResult<String, SimpleMessage> result) {
                    // Вычисляем время отправки и логируем метаданные записи (partition, offset и т.д.)
                    Instant end = Instant.now();
                    long durationMs = Duration.between(start, end).toMillis();
                    RecordMetadata meta = result.getRecordMetadata();
					log.info("Produced message id={} to topic={} partition={} offset={} timestamp={} durationMs={} payloadBytes={}",
                            id,
                            meta.topic(),
                            meta.partition(),
                            meta.offset(),
                            meta.timestamp(),
                            durationMs,
                            payloadBytes);
                    // Для отладки можно залогировать полный объект сообщения
                    log.debug("Produced full message payload: {}", message);
                }

                @Override
                public void onFailure(Throwable ex) {
                    // При неудаче логируем ошибку и время, затраченное на попытку отправки
                    Instant end = Instant.now();
                    long durationMs = Duration.between(start, end).toMillis();
                    log.error("Failed to produce message id={} topic={} after {} ms: {}",
                            id, topic, durationMs, ex.toString());
                    // Подробный stacktrace в debug-логе для анализа причин
                    log.debug("Failure details for message id={}", id, ex);
                }
            });

        } catch (Exception ex) {
            // Обработка синхронных ошибок, например проблем при сериализации или конфигурации KafkaTemplate
            log.error("Synchronous error when sending message id={} to topic={}: {}", id, topic, ex.toString());
            log.debug("Stacktrace:", ex);
        }
    }
}