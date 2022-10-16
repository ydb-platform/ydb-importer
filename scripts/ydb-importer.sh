#! /bin/sh

# export YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=$(pwd)/yc-fednew-iot1sa1.json

# -Djavax.net.ssl.trustStore=work-cas.dat -Djavax.net.ssl.trustStorePassword=passw0rd
java -Xms256m -Xmx2048m -jar lib/ydb-importer-* $@
