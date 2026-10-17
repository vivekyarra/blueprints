$ErrorActionPreference = 'Stop'
$qaDir = $PSScriptRoot
$repoDir = (Resolve-Path (Join-Path $qaDir '../..')).Path
foreach ($name in @('kafka-state-kestra', 'kafka-state-client', 'kafka-state-broker', 'kafka-state-postgres')) {
    docker container inspect $name *> $null
    if ($LASTEXITCODE -eq 0) { throw "Fixture $name already exists; use a clean Docker context." }
}
New-Item -ItemType Directory -Path (Join-Path $qaDir 'plugins') -Force | Out-Null
docker run --rm -v "${qaDir}/plugins:/app/plugins" kestra/kestra:v2.0.6-no-plugins plugins install io.kestra.plugin:plugin-jdbc-postgres:2.4.0 io.kestra.plugin:plugin-script-python:1.17.0
if ($LASTEXITCODE -ne 0) { throw 'Plugin installation failed' }
docker network create kafka-state-qa
if ($LASTEXITCODE -ne 0) { throw 'Use a fresh kafka-state-qa network' }
docker run -d --name kafka-state-broker --network kafka-state-qa -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka-state-broker:9092 -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka-state-broker:9093 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 -e CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk confluentinc/cp-kafka:8.2.2
if ($LASTEXITCODE -ne 0) { throw 'Kafka start failed' }
docker run -d --name kafka-state-postgres --network kafka-state-qa -e POSTGRES_PASSWORD=kafka_state_fixture postgres:16
if ($LASTEXITCODE -ne 0) { throw 'PostgreSQL start failed' }
python (Join-Path $qaDir 'setup_runtime.py')
if ($LASTEXITCODE -ne 0) { throw 'Fixture secret setup failed' }
docker run -d --name kafka-state-kestra --user root --network kafka-state-qa -p 127.0.0.1:18081:8080 --env-file "${qaDir}/runtime.env" -v "${qaDir}/plugins:/app/data/plugins" -v "${qaDir}/application.yaml:/etc/config/application.yaml" -v /var/run/docker.sock:/var/run/docker.sock kestra/kestra:v2.0.6-no-plugins server local --config /etc/config/application.yaml
if ($LASTEXITCODE -ne 0) { throw 'Kestra start failed' }
docker run -d --name kafka-state-client --network kafka-state-qa -v "${qaDir}:/qa" -v "${repoDir}/flows:/flows" python:3.12-slim sleep infinity
if ($LASTEXITCODE -ne 0) { throw 'Test client start failed' }
docker exec kafka-state-client pip install --quiet kestra==1.0.0 confluent-kafka==2.12.2 'psycopg[binary]==3.2.12' pyyaml requests
if ($LASTEXITCODE -ne 0) { throw 'Test dependency installation failed' }
Write-Output 'Wait for Kafka and http://localhost:18081, then run the commands in README.md.'
