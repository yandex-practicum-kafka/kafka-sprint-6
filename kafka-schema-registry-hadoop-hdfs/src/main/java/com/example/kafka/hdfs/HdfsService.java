package com.example.kafka.hdfs;

import com.example.kafka.config.HdfsProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

/**
 * Сервис для записи строк в HDFS.
 * Формирует уникальные файлы по дате и записывает переданную строку в отдельный файл.
 * Использует инжектируемый Hadoop FileSystem и свойства из HdfsProperties.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class HdfsService {

    // Инстанс Hadoop FileSystem, конфигурируется через Spring (HDFS URI и т.д.)
    private final FileSystem fileSystem;

    // Настройки HDFS (директория, префикс файла, расширение)
    private final HdfsProperties props;

    // Формат даты для организации файлов по директориям: yyyy-MM-dd
    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /**
     * Записывает строку в уникальный файл в HDFS.
     * Файлы группируются по дате: {props.dir}/yyyy-MM-dd/
     * Имя файла содержит префикс, timestamp и UUID для уникальности.
     *
     * @param line содержимое для записи (одна строка)
     * @throws IOException при ошибках взаимодействия с HDFS
     */
    public void writeMessageUniqueFile(String line) throws IOException {

        // Текущая дата для структуры папок
        String date = LocalDate.now().format(DATE_FMT);
        String dirPath = String.format("%s/%s", props.getDir(), date);

        // Формируем уникальное имя файла: prefix/timestamp-uuid.extension
        String fileName = String.format("%s/%s%s-%s%s",
                dirPath,
                props.getFilePrefix(),
                System.currentTimeMillis(),
                UUID.randomUUID().toString(),
                props.getFileExtension());

        Path dir = new Path(dirPath);
        Path path = new Path(fileName);

        log.debug("HDFS write requested. dir={}, path={}", dirPath, fileName);

        // Добавляем перевод строки и вычисляем байты для потока
        byte[] bytes = (line + "\n").getBytes(StandardCharsets.UTF_8);
        Instant start = Instant.now();

        // Синхронизация по fileSystem предотвращает одновременную модификацию внутренних структур Hadoop client'а
        synchronized (fileSystem) {
            try {
                // Если директория не существует — создаём её рекурсивно
                if (!fileSystem.exists(dir)) {
                    boolean created = fileSystem.mkdirs(dir);
                    log.info("HDFS directory created: {} (result={})", dirPath, created);
                } else {
                    log.debug("HDFS directory already exists: {}", dirPath);
                }

                // Создаём файл и записываем данные; try-with-resources гарантирует закрытие потока
                try (FSDataOutputStream out = fileSystem.create(path)) {
                    out.write(bytes);
                    // hflush обеспечивает, что данные сброшены до DataNode (не обязательно полностью синхронизированы на диск)
                    out.hflush();
                }

                // Получаем метаданные записанного файла для логирования (длина, replication, blockSize)
                FileStatus status = fileSystem.getFileStatus(path);
                long length = status.getLen();
				short replication = status.getReplication();
                long blockSize = status.getBlockSize();

                Instant end = Instant.now();
                long durationMs = Duration.between(start, end).toMillis();

                log.info("Wrote file to HDFS: path={} len={} replication={} blockSize={} durationMs={}",
                        path.toString(), length, replication, blockSize, durationMs);
                // Логируем часть содержимого файла для отладки (до 500 символов)
                log.debug("Written content (first 500 chars): {}",
                        line.length() > 500 ? line.substring(0, 500) + "..." : line);

            } catch (IOException e) {
                // Обрабатываем ошибки ввода-вывода и повторно пробрасываем для вызывающего кода
                log.error("IOException writing to HDFS path={} dir={} : {}", path, dirPath, e.toString());
                log.debug("HDFS write error stacktrace:", e);
                throw e;
            } catch (Exception e) {
                // Неожиданные ошибки оборачиваем в IOException, чтобы сохранить контракт метода
                log.error("Unexpected error writing to HDFS path={} dir={} : {}", path, dirPath, e.toString());
                log.debug("HDFS unexpected error stacktrace:", e);
                throw new IOException("Unexpected error in HDFS write", e);
            }
        }
    }
}