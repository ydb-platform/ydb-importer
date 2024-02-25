#! /bin/sh

# For YDB in Yandex Cloud:
#   https://cloud.yandex.com/en/docs/iam/operations/authorized-key/create
# export YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=$(pwd)/yc-fednew-iot1sa1.json

java -Xms256m -Xmx8192m -classpath 'lib/*' tech.ydb.importer.YdbImporter $@
