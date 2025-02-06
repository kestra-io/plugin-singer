echo $GOOGLE_SERVICE_ACCOUNT | base64 -d > src/test/resources/gcp-service-account.json

mkdir data
curl -s -o data/mysql.sql https://raw.githubusercontent.com/harryho/db-samples/master/mysql/northwind.sql
curl -s -o data/postgres.sql https://raw.githubusercontent.com/harryho/db-samples/master/pgsql/northwind.sql
curl -s -o data/mongo.jsonl https://raw.githubusercontent.com/ozlerhakan/mongodb-json-files/master/datasets/books.json
curl -s -o data/sqlserver.sql https://raw.githubusercontent.com/microsoft/sql-server-samples/master/samples/databases/northwind-pubs/instnwnd.sql

curl -s -o data/oracle.sql https://raw.githubusercontent.com/oracle-samples/db-sample-schemas/main/customer_orders/co_create.sql
curl -s -o data/oracle_pop.sql https://raw.githubusercontent.com/oracle-samples/db-sample-schemas/main/customer_orders/co_populate.sql
cat data/oracle_pop.sql >> data/oracle.sql
rm data/oracle_pop.sql

docker compose -f docker-compose-ci.yml up -d
sleep 30
docker compose -f docker-compose-ci.yml exec postgres sh -c "psql -d postgres -U postgres -f /tmp/docker/postgres.sql > /dev/null"
docker compose -f docker-compose-ci.yml exec postgres sh -c "psql -d postgres -U postgres -c 'CREATE DATABASE sync' > /dev/null"
docker compose -f docker-compose-ci.yml exec mongo sh -c "mongoimport --authenticationDatabase admin -c books  "mongodb://root:example@localhost/samples" /tmp/docker/mongo.jsonl > /dev/null"
docker compose -f docker-compose-ci.yml exec mysql sh -c "mysql -u root -pmysql_passwd < /tmp/docker/mysql.sql"
docker compose -f docker-compose-ci.yml exec sqlserver sh -c "/opt/mssql-tools18/bin/sqlcmd -S localhost -U SA -P SQLServer_Passwd -C -d msdb -i /tmp/docker/sqlserver.sql > /dev/null"
docker compose -f docker-compose-ci.yml exec oracle sh -c "sqlplus -s system/oracle_passwd@localhost @/tmp/docker/oracle.sql > /dev/null"
