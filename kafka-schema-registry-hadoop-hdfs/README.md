## Задание 2. Интеграция Kafka с внешними системами (Apache NiFi / Hadoop)

### Цель выполнения задания

Цель выполнения задания — развернуть отказоустойчивый Kafka‑кластер (3 брокера), настроить топик с 3 партициями и репликацией 3, подключить Schema Registry и зарегистрировать Avro‑схему, затем обеспечить поток данных: `producer` → `Kafka` → `consumer` → `HDFS`, и продемонстрировать успешную доставку и запись в HDFS.  

### Описание проекта/приложения

- Приложение состоит из двух сервисов (app-producer и app-consumer), поднятых через docker‑compose и собранных из Dockerfile как один образ `spring-kafka-app`. Producer в профиле `producer` периодически генерирует Avro‑сообщения и отправляет их в Kafka через три брокера (KRaft кластер `kafka-1,2,3`) с использованием Schema Registry для управления схемой. Consumer в профиле consumer читает сообщения из того же топика, десериализует через Schema Registry и записывает содержимое в HDFS (кластера Hadoop — NameNode + 3 DataNode), используя HDFS client (Hadoop API) и параметры из окружения (HDFS_URI, HADOOP_USER_NAME и т.д.).  
  
- Инфраструктура в [docker-compose.yml](docker-compose.yml): три брокера `bitnami/kafka` в KRaft режиме, Confluent Schema Registry, веб‑интерфейс Kafka UI, NameNode и три DataNode (`apache/hadoop`), плюс два экземпляра приложения. Конфиги Hadoop (`core-site.xml` и `hdfs-site‑*.xml`), скрипты `entrypoint.sh` и avro‑схема ([src/main/avro/SimpleMessage.avsc](src/main/avro/SimpleMessage.avsc)) монтируются в контейнеры.  
  
### Структура проекта  

```
./
├── .env                                   # Переменные окружения для docker-compose (версии образов, порты, креды)
├── docker-compose.yml                     # Compose‑файл для запуска Zookeeper/Kafka/SchemaRegistry + HDFS (NameNode + 3 DataNode) + producer/consumer
├── Dockerfile                             # Multi‑stage Dockerfile для сборки и запуска Java Spring/Gradle приложения (producer/consumer)
├── README.md                              # Документация: как собрать, запустить и тестировать end‑to‑end (producer → Kafka → consumer → HDFS)
├── output.txt                             # Пример выходных логов/результатов работы (демо output)
├── payload.json                           # Пример JSON payload для тестирования producer
├── settings.gradle                        # Gradle settings (имя проекта, multi‑module если есть)
├── gradlew                                # Gradle wrapper (Unix) — запускает сборку с фиксированной версией gradle
├── gradlew.bat                            # Gradle wrapper (Windows)
├── gradle/                                # Gradle wrapper files
│   └── wrapper/
│       ├── gradle-wrapper.jar
│       └── gradle-wrapper.properties
├── gradle/                                # Доп. папка с конфигурацией gradle (если нужна)
│
├── images/                                # Скриншоты и визуальные примеры работы системы
├── images/                                # Скриншоты и визуальные примеры работы системы
│  ├── docker/                             # Скриншоты, связанные с Docker-контейнерами
│  │  ├── docker-consumer-logs.png               # Логи Docker-контейнера Kafka Consumer. Демонстрируют успешное потребление сообщений из Kafka.
│  │  ├── docker-producer-logs.png               # Логи Docker-контейнера Kafka Producer. Действия по отправке сообщений в Kafka.
│  │  ├── docker-schema-registry-logs.png        # Логи Docker-контейнера Schema Registry. Операции регистрации и управления схемами данных.
│  │  ├── docker-datanode-1-logs.png             # Логи DataNode 1 Hadoop кластера, работающего в Docker.
│  │  ├── docker-datanode-2-logs.png             # Логи DataNode 2 Hadoop кластера, работающего в Docker.
│  │  ├── docker-datanode-3-logs.png             # Логи DataNode 3 Hadoop кластера, работающего в Docker.
│  │  ├── docker-namenode-logs.png               # Логи NameNode Hadoop кластера, работающего в Docker.
│  │  ├── docker-services-all.png                # Общий вид запущенных Docker-контейнеров. Показывает, какие сервисы (Kafka, Zookeeper, Hadoop) запущены.
│  │  ├── docker-services-all-logs.png           # Общие логи Docker-контейнеров, вероятно, показывающие запуск и инициализацию сервисов.
│  │  ├── docker-desktop-all-services-logs.png   # Общие логи Docker Desktop, возможно, связанные с управлением контейнерами и сетью.
│  ├── hadoop/                                       # Скриншоты, связанные с пользовательским интерфейсом Hadoop
│  │  ├── hadoop-hdfs-console.png                    # Скриншот консоли HDFS. Команды, выполненные в HDFS.
│  │  ├── hadoop-ui-datanode-1-2-3-information.png   # Скриншот UI Hadoop, показывающий информацию о всех трех DataNode.
│  │  └── hadoop-ui-datanode-1-overview.png          # Скриншот UI Hadoop, показывающий обзор DataNode 1. Информация о дисках, памяти и т.д.
│  │  ├── hadoop-ui-datanode-2-overview.png          # Скриншот UI Hadoop, показывающий обзор DataNode 2.
│  │  ├── hadoop-ui-datanode-3-overview.png          # Скриншот UI Hadoop, показывающий обзор DataNode 3.
│  │  ├── hadoop-ui-hdfs-directory.png               # Скриншот UI Hadoop, показывающий структуру директорий в HDFS.
│  │  ├── hadoop-ui-hdfs-file.png                    # Скриншот UI Hadoop, показывающий содержимое файла в HDFS.
│  │  ├── hadoop-ui-hdfs.png                         # Скриншот UI Hadoop, показывающий общий вид HDFS.
│  │  └── hadoop-ui-namenode-overview.png            # Скриншот UI Hadoop, показывающий обзор NameNode. Информация о кластере, ресурсах и т.д.
│  ├── kafka/                                    # Скриншоты, связанные с пользовательским интерфейсом Kafka
│  │  ├── kafka-ui-brokers.png                   # Скриншот UI Kafka, показывающий список брокеров в кластере.
│  │  ├── kafka-ui-topic-messages.png            # Скриншот UI Kafka, показывающий сообщения в определенной теме.
│  │  └── kafka-ui-topics.png                    # Скриншот UI Kafka, показывающий список тем в кластере.
│  ├── schema-registry/                          # Скриншоты, связанные с Schema Registry
│  │  └── schema-registry-subjects.png           # Скриншот Schema Registry, показывающий список зарегистрированных схем.
│  │
├── config/                                      # Hadoop конфигурации (монтируются в Hadoop контейнеры)
│   ├── core-site.xml                            # fs.defaultFS, tmp dir, и др. (монтируется в все узлы)
│   ├── hdfs-site-namenode.xml                   # Конфиг NameNode (dfs.namenode.name.dir, replication и пр.)
│   ├── hdfs-site-datanode-1.xml                 # Конфиг DataNode #1 (data dirs, ports)
│   ├── hdfs-site-datanode-2.xml                 # Конфиг DataNode #2
│   └── hdfs-site-datanode-3.xml                 # Конфиг DataNode #3
│
├── datanode_entrypoint.sh                       # Entrypoint для DataNode (ожидание NameNode, подготовка директорий, запуск)
├── namenode_entrypoint.sh                       # Entrypoint для NameNode (форматирование при первом запуске, запуск демона)
│
├── src/
│   └── main/
│       ├── avro/
│       │   └── SimpleMessage.avsc               # Avro схема для сообщений (используется Avro serializer + Schema Registry)
│       ├── java/
│       │   └── com/
│       │       └── example/
│       │           └── kafka/
│       │               ├── KafkaApplication.java          # Точка входа Spring Boot (main)
│       │               ├── config/
│       │               │   ├── AppProperties.java         # Бин свойств приложения (bootstrap, schemaRegistry, hdfs)
│       │               │   ├── KafkaConfig.java           # Producer/Consumer config, сериализаторы Avro/SchemaRegistry
│       │               │   ├── KafkaTopicConfig.java      # (Опционально) admin client для создания топиков
│       │               │   ├── HdfsConfig.java            # Конфигурация HDFS (FileSystem бин)
│       │               │   └── HdfsProperties.java        # Свойства HDFS (uri, target dir, user)
│       │               ├── producer/
│       │               │   └── KafkaProducerService.java  # Сервис отправки Avro сообщений в Kafka
│       │               ├── consumer/
│       │               │   └── KafkaConsumerService.java  # Kafka listener: десериализация Avro -> HdfsService
│       │               └── hdfs/
│       │                   └── HdfsService.java           # Логика записи/аппенда в HDFS (temp->rename, партиции)
│       └── resources/
│           └── application.yml                            # Spring profiles и свойства (producer/consumer): Kafka, Schema Registry, HDFS, logging
│
└── (прочие/необязательные) файлы
    ├── build.gradle                           # основная конфигурация Gradle сборки (плагины: avro, spring-boot, etc.)
    ├── settings.gradle                        # доп. настройки Gradle сборки
    └── other scripts/configs                  # любые дополнительные утилиты/скрипты (например миграции, init scripts)
```

#### Краткие пояснения по ключевым файлам и их назначению:

- **[.env](.env)**: держит параметры версий и кредов, используемые docker-compose (удобно переключать окружения).  
- **[docker-compose.yml](docker-compose.yml)**: поднимает Zookeeper, Kafka, Schema Registry, NameNode и три DataNode, плюс два контейнера приложения (producer и consumer) собранных из Dockerfile.  
- **[Dockerfile](Dockerfile)**: multi‑stage — первый этап билд Gradle (bootJar), второй — запуск fat‑jar на JRE.  
- **config/*.xml**: Hadoop конфигурации — обязательны для корректной регистрации DataNode в NameNode и правильной адресации HDFS.  
- **[datanode_entrypoint.sh](datanode_entrypoint.sh) / [namenode_entrypoint.sh](namenode_entrypoint.sh)**: логика корректного старта кластеров (форматирование), полезно автоматизировать первый запуск.  
- **[src/main/avro/SimpleMessage.avsc](src/main/avro/SimpleMessage.avsc)**: исходная схема сообщений — с ней регистрирует schema registry и генерирует Avro Java классы при сборке.  
- **[src/main/java/...](src/main/java/com/example/kafka)**: Spring Boot приложение, которое в профиле producer генерирует/шлёт Avro сообщения в Kafka через Schema Registry; в профиле consumer — читает сообщения и пишет в HDFS (через HdfsService).  
- **[application.yml](src/main/resources/application.yml)**: содержит профили producer/consumer, конфигурацию bootstrap.servers, schema.registry.url, hdfs.uri, топики и параметры сериализации.  
- **images/**: документационные скриншоты, полезны для README и быстрой проверки UI.  

#### Конфигурация HDFS-кластера

**Набор конфигураций и entrypoint-скриптов для запуска простого HDFS-кластера в контейнерах**: один NameNode и несколько DataNode. Файлы задают URI файловой системы, директории хранения и сетевые/HTTP-порты, а скрипты подготавливают каталоги и запускают процессы.  

[datanode_entrypoint.sh](datanode_entrypoint.sh)  
- Создаёт каталог `/usr/local/hadoop/hdfs/datanode` с правами 777 и выполняет переданную команду; гарантирует наличие директории для блоков DataNode перед стартом.  

[namenode_entrypoint.sh](namenode_entrypoint.sh)  
- Создаёт /usr/local/hadoop/hdfs/namenode, выполняет `hdfs namenode -format` и затем запускает основной процесс; форматировать NameNode нужно только при первичной инициализации (иначе потеря метаданных).  

[core-site.xml](core-site.xml)  
- Устанавливает `fs.defaultFS = hdfs://hadoop-namenode:9000` — основной URI HDFS, который должны использовать клиенты и сервисы.  

[hdfs-site-datanode-1.xml](hdfs-site-datanode-1.xml) (и -2, -3)  
- Указывает локальную директорию для блоков (`dfs.datanode.data.dir`), hostname DataNode и bind-порты для передачи данных и HTTP; порты кастомные, убедитесь в их доступности и согласованности с сетевой конфигурацией.  

`hdfs-site-namenode.xml`  
- Задаёт `dfs.replication = 3` и директорию для метаданных NameNode (`dfs.namenode.name.dir`); `replication=3` требует минимум трёх работающих DataNode и персистентного хранилища для name dir.  

### Основные шаги, запуск  

1. Развёртывание инфраструктуры:  
   - Запуск: `docker-compose up --build`  
   - Поднимаются сервисы `kafka-1,2,3`, `schema-registry`, `kafka-ui`, `hadoop-namenode` и `hadoop-datanode-1,2,3`, `app-producer` и `app-consumer`.  

2. Создание топика (пример команды внутри одного из брокеров или локально через kafka-topics):  
   - `kafka-topics.sh --bootstrap-server kafka-1:9092 --create --topic test-topic --partitions 3 --replication-factor 3`  

3. Регистрация схемы в Schema Registry:  

```
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
	--data '{"schema":"<json-escaped-contents-of-SimpleMessage.avsc>"}' \
	http://localhost:8081/subjects/test-topic-value/versions
```
   (или через REST Schema Registry в контейнере schema-registry)  

4. Producer:  
   - Spring‑сервис в профиле producer читает `SPRING_KAFKA_BOOTSTRAP_SERVERS` и `SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_URL`, использует Avro serializer (Confluent) и публикует сообщения в топик `test-topic`.  

5. Consumer:  
   - Spring‑listener читает Avro сообщения, преобразует в доменный объект и вызывает HdfsService, который записывает данные в каталог в HDFS (`HDFS_DIR`, например /data). Процесс записи: `write` -> `flush/close` -> `(optionally) rename` для атомарности.  

6. Верификация/уровень‑вывода:  

Ожидаемые логи и подтверждение успешной работы  

- Скриншот `docker ps / docker‑desktop` с запущенными сервисами.  
- Вывод `kafka-console-consumer`, где видны приходящие сообщения.  
- Логи producer: подтверждение отправки (`offsets`, `key/partition info`).  
- Логи consumer: подтверждение получения сообщений и успешного вызова HdfsService.  
- Вывод `hdfs dfs -ls /data` и `hdfs dfs -cat /data/<файл>` — подтверждение записи.  
- Логи NameNode/DataNode с информацией о записанных блоках.  

Подробнее:  

   - Проверить топик через сервис Kafka UI (http://localhost:8080).  

   - Просматривать сообщения через `kafka-console-consumer`:  
     `kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --from-beginning`  

   - Логи `producer/consumer`: `docker-compose logs -f spring-kafka-producer / spring-kafka-consumer`  

   - Проверка HDFS: в контейнере namenode или через `hdfs-client`:  
```
     docker exec -it hadoop-namenode hdfs dfs -ls /data
     docker exec -it hadoop-namenode hdfs dfs -cat /data/<file>
```
   - Логи Hadoop: `docker-compose logs -f hadoop-namenode / hadoop-datanode-1`  

### Артефакты, подтверждающие выполнение

- [docker-compose.yml](docker-compose.yml) — содержит конфигурацию 3 брокеров Kafka, Schema Registry, `kafka-ui`, NameNode и 3 DataNode, а также `app-producer/consumer`.  

- [Dockerfile](Dockerfile) — сборка Spring приложения.  

- [config/core-site.xml](config/core-site.xml) и `config/hdfs-site-*.xml` — конфигурации Hadoop.  

- [namenode_entrypoint.sh](namenode_entrypoint.sh) и [datanode_entrypoint.sh](datanode_entrypoint.sh) — скрипты запуска HDFS-узлов (форматирование/ожидание).  

- [src/main/avro/SimpleMessage.avsc](src/main/avro/SimpleMessage.avsc) — Avro‑схема, зарегистрированная в Schema Registry.  

- [src/main/java/...](src/main/java/com/example/kafka) — код приложения: [KafkaProducerService](src/main/java/com/example/kafka/producer/KafkaProducerService.java) (публикация Avro), [KafkaConsumerService](src/main/java/com/example/kafka/consumer/KafkaConsumerService.java) (чтение и передача в HDFS), [HdfsService](src/main/java/com/example/kafka/hdfs/HdfsService.java) (запись в HDFS).  

- [README.md](README.md) — инструкции, примеры команд и пример логов/скриншотов.  

### Скриншоты и логи

Набор изображений из проекта: логи контейнеров, UI Kafka/Hadoop и снимки HDFS. Ниже — встроенные изображения с короткими подписями (файлы находятся в каталоге images/).  

#### Docker — логи и сервисы
  
![docker-services-all](images/docker-services-all.png)  
Список запущенных Docker‑контейнеров (docker ps / docker‑compose ps). Показывает, какие сервисы подняты.  
  
![docker-services-all-logs](images/docker-services-all-logs.png)  
Агрегированные логи всех контейнеров. Быстрый обзор статуса старта и ошибок.  
  
![docker-desktop-all-services-logs](images/docker-desktop-all-services-logs.png)  
Логи Docker Desktop/агрегат. Диагностика управления контейнерами и сетевых проблем.  
  
#### Docker — логи сервисов Consumer, Producer
  
![docker-consumer-logs](images/docker-consumer-logs.png)  
Логи Docker‑контейнера Kafka‑Consumer. Показывают подключение к топику и успешное чтение сообщений.  
  
![docker-producer-logs](images/docker-producer-logs.png)  
Логи Docker‑контейнера Kafka‑Producer. Демонстрируют успешную отправку сообщений (acks, partition, offset).  
  
#### Docker — логи сервиса, Schema Registry
  
![docker-schema-registry-logs](images/docker-schema-registry-logs.png)  
Логи контейнера Schema Registry. Старт сервиса, регистрация/валидация схем.  
  
#### Docker — логи сервисов, Hadoop / HDFS (Namenode, Datanode-1,2,3)
    
![docker-namenode-logs](images/docker-namenode-logs.png)  
Логи NameNode (Hadoop). Инициализация метаданных, safe mode, RPC и web UI адрес.  
  
![docker-datanode-1-logs](images/docker-datanode-1-logs.png)  
Логи DataNode #1 (Hadoop). Регистрация на NameNode, heartbeat и работа с блоками.  
  
![docker-datanode-2-logs](images/docker-datanode-2-logs.png)
Логи DataNode #2 (Hadoop). Подтверждение распределённого хранения и синхронизации блоков.  
  
![docker-datanode-3-logs](images/docker-datanode-3-logs.png)  
Логи DataNode #3 (Hadoop). Дополнительная нода для отказоустойчивости/репликации.  
  
#### Hadoop / HDFS — UI и структура, веб-консоль
  
![hadoop-hdfs-console](images/hadoop-hdfs-console.png)  
HDFS console / командная работа с файловой системой. Примеры операций (ls, put, get).  
  
![hadoop-ui-namenode-overview](images/hadoop-ui-namenode-overview.png)  
NameNode UI — overview. Состояние кластера, live/dead nodes, количество блоков.  
  
![hadoop-ui-datanode-1-2-3-information](images/hadoop-ui-datanode-1-2-3-information.png)  
UI Hadoop — сводная информация по трём DataNode. Статусы, дисковая нагрузка и heartbeat.  
  
![hadoop-ui-datanode-1-overview](images/hadoop-ui-datanode-1-overview.png)  
Hadoop UI — обзор DataNode #1. Подробные метрики ноды (disk, I/O, last heartbeat).  
  
![hadoop-ui-datanode-2-overview](images/hadoop-ui-datanode-2-overview.png)  
Hadoop UI — обзор DataNode #2. Сравнение с другими нодами по нагрузке.  
  
![hadoop-ui-datanode-3-overview](images/hadoop-ui-datanode-3-overview.png)  
Hadoop UI — обзор DataNode #3. Подтверждение репликации и распределения блоков.  
  
#### Представление файловой системы
  
![hadoop-ui-hdfs](images/hadoop-ui-hdfs.png)  
HDFS UI — общий вид файловой системы. Навигация и статистика использования.  
  
![hadoop-ui-hdfs-directory](images/hadoop-ui-hdfs-directory.png)  
HDFS UI — просмотр директории. Список файлов, права, размещение блоков.  
  
![hadoop-ui-hdfs-file](images/hadoop-ui-hdfs-file.png)  
HDFS UI — метаданные конкретного файла. Размер, блоки, расположение по DataNode, checksum.  

#### Kafka UI 

![kafka-ui-brokers](images/kafka-ui-brokers.png)  
Kafka UI — список брокеров в кластере. ID брокеров, состояние leader/follower и распределение партиций.

![kafka-ui-topics](images/kafka-ui-topics.png)  
Kafka UI — список топиков. Кол‑во партиций, replication factor и политики хранения.

![kafka-ui-topic-messages](images/kafka-ui-topic-messages.png)  
Kafka UI — просмотр сообщений в топике. Примеры payload, offset, partition и timestamp.

#### Schema Registry

![schema-registry-subjects](images/schema-registry-subjects.png)  
Schema Registry — список subjects. Подтверждение регистрации схем и доступности endpoint /subjects.

### Вывод, заключение по выполненной работе

- Результат: успешно развернут Kafka‑кластер из трёх брокеров в KRaft режиме с настроенным топиком test-topic (3 партиции, rf=3), развернут Schema Registry и зарегистрирована Avro‑схема; producer публикует Avro‑сообщения, consumer читает их и записывает в распределённое хранилище HDFS. Интеграция между Kafka и Hadoop отлажена: сообщения проходят через Kafka и оказываются записанными в HDFS, что подтверждается логами и выводом hdfs dfs.  

- Достоинства и наблюдения: архитектура надёжна благодаря репликации и разделению на партиции; Schema Registry обеспечивает совместимость схем; HDFS даёт долговременное хранение и распределённое размещение блоков по DataNode.  

- Итог: поставленная задача по развёртыванию Kafka + Schema Registry и интеграции с Hadoop выполнена; все пункты проверки (создание топика, регистрация схемы, успешная передача и запись в HDFS) доступны и подтверждаемы приведёнными командами и логами.  
