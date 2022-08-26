Утилита импорта данных из источников JDBC в базу данных YDB.
На сегодня поддерживаются источники Oracle Database и PostgreSQL.

Требуется Java 11 или выше (техничеки может заработать и на более ранних,
если перекомпилировать).

Настройка: см. примеры и комментарии в файлах sample-oracle.xml, sample-postgres.xml.

Запуск: см. пример команды в файле ydb-import.sh
Технически запускается класс ydb.importer.YdbImporter, 
которому необходимо в качестве аргумента передать имя файла настроек.

Описание формата файла конфигурации:

<?xml version="1.0" encoding="UTF-8"?>
<ydb-importer>
    <workers>
        <!-- Количество рабочих потоков (целое число от 1 и выше).
             Также соответствует максимальному устанавливаемому количеству соединений с источником,
             и максимальному количеству сессий с получателем.
         -->
        <pool size="4"/>
    </workers>
    <!-- Параметры подключения к БД-источнику.
         type - обязательный атрибут, влияющий на логику взаимодействия с источником
      -->
    <source type="oracle|postgresql">
        <jdbc-class>oracle.jdbc.driver.OracleDriver</jdbc-class>
        <jdbc-url>jdbc:oracle:thin:@//hostname:1521/serviceName</jdbc-url>
        <username>demo1</username>
        <password>passw0rd</password>
    </source>
    <!-- Параметры подключения к БД-получателю.
         Данные аутентификации необходимо установить в переменных окружения,
         как описано в документации: https://ydb.tech/ru/docs/reference/ydb-sdk/auth#env
         Если используется YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS, то файл ключа надо
         генерировать как написано здесь:
         https://cloud.yandex.ru/docs/iam/operations/authorized-key/create
          -->
    <target type="ydb">
        <!-- Выгрузить скрипт создания таблиц в указанный файл. 
             Может использоваться в том числе при отсутствии connection-string  -->
        <script-file>sample-oracle.yql.tmp</script-file>
        <!-- Строка соединения: endpoint + база данных -->
        <connection-string>grpcs://ydb.serverless.yandexcloud.net:2135?database=/ru-central1/b1gfvslmokutuvt2g019/etn63999hrinbapmef6g</connection-string>
        <!-- Удалять ли уже существующие таблицы с теми же именами, что предполагается заливать -->
        <replace-existing>true</replace-existing>
        <!-- Заливать ли данные в таблицы после их создания или пересоздания -->
        <load-data>true</load-data>
        <!-- Максимальная порция заливки обычных данных, в строках -->
        <max-batch-rows>1000</max-batch-rows>
        <!-- Максимальная порция заливки BLOB-данных, в строках -->
        <max-blob-rows>200</max-blob-rows>
    </target>
    <!-- Настройки преобразования структуры исходных таблиц.
         Устанавливаются централизованно с присвоением имени,
         затем используются для конкретной группы отбираемых таблиц.   -->
    <table-options name="default">
        <!--  Возможные значения case-mode: ASIS (по умолчанию), LOWER, UPPER.
              Влияет на регистр подставляемых в шаблоны имён.  -->
        <case-mode>ASIS</case-mode>
        <!-- Шаблон полного имени таблицы, включая каталог размещения.
             Используются подстановочные имена ${schema} и ${table},
             соответствующие схеме и имени копируемой исходной таблицы. -->
        <table-name-format>oraimp1/${schema}/${table}</table-name-format>
        <!-- Шаблон полного имени таблицы, включая каталог размещения.
             Используются подстановочные имена ${schema}, ${table} и ${field},
             соответствующие схеме, имени копируемой исходной таблицы и имени
             поля типа BLOB. -->
        <blob-name-format>oraimp1/${schema}/${table}_${field}</blob-name-format>
        <!-- Возможные значения: DATE (по умолчанию), INT, STR -->
        <conv-date>INT</conv-date>
    </table-options>
    <!-- Фильтр для отбора копируемых таблиц с источника -->
    <table-map options="default">
        <!-- Включаемые схемы -->
        <include-schemas regexp="true">.*</include-schemas>
        <!-- Исключаемые схемы -->
        <exclude-schemas>SOMESCHEMA</exclude-schemas>
        <!-- Также могут присутствовать include-tables и exclude-tables, 
             для явного указания фильтра по именам таблиц
             и/или регулярным выражениям над именами таблиц. -->
    </table-map>
    <!-- Конкретный запрос или указание ссылки на конкретную таблицу -->
    <table-ref options="default">
        <schema-name>ora$sys</schema-name>
        <table-name>all_tables</table-name>
        <!-- Если указан запрос, он выполняется как написано -->
        <query-text>SELECT * FROM all_tables</query-text>
        <!-- Для запроса стоит явно определить ключевые колонки.  -->
        <key-column>OWNER</key-column>
        <key-column>TABLE_NAME</key-column>
    </table-ref>
</ydb-importer>