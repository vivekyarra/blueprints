$ErrorActionPreference = 'Stop'
$qaDir = $PSScriptRoot
foreach ($name in @('csv-snapshot-kestra', 'csv-snapshot-files', 'kafka-state-postgres')) {
    docker container inspect $name *> $null
    if ($LASTEXITCODE -eq 0) { throw "Fixture $name already exists; use a clean Docker context." }
}
New-Item -ItemType Directory -Path (Join-Path $qaDir 'plugins') -Force | Out-Null
New-Item -ItemType Directory -Path (Join-Path $qaDir 'fixtures') -Force | Out-Null
docker run --rm -v "${qaDir}/plugins:/app/plugins" kestra/kestra:v2.0.6-no-plugins plugins install io.kestra.plugin:plugin-jdbc-postgres:2.4.0 io.kestra.plugin:plugin-serdes:2.2.1
if ($LASTEXITCODE -ne 0) { throw 'Plugin installation failed' }
docker network create kafka-state-qa
if ($LASTEXITCODE -ne 0) { throw 'Use a fresh kafka-state-qa network' }
docker run -d --name kafka-state-postgres --network kafka-state-qa -e POSTGRES_PASSWORD=kafka_state_fixture postgres:16
if ($LASTEXITCODE -ne 0) { throw 'PostgreSQL start failed' }
python (Join-Path $qaDir 'setup_runtime.py')
if ($LASTEXITCODE -ne 0) { throw 'Secret setup failed' }
docker run -d --name csv-snapshot-kestra --user root --network kafka-state-qa -p 127.0.0.1:18082:8080 --env-file "${qaDir}/runtime.env" -v "${qaDir}/plugins:/app/data/plugins" -v "${qaDir}/application.yaml:/etc/config/application.yaml" kestra/kestra:v2.0.6-no-plugins server local --config /etc/config/application.yaml
if ($LASTEXITCODE -ne 0) { throw 'Kestra start failed' }
docker run -d --name csv-snapshot-files --network kafka-state-qa -v "${qaDir}/fixtures:/fixtures" python:3.12-slim python -m http.server 8000 --directory /fixtures
if ($LASTEXITCODE -ne 0) { throw 'CSV server start failed' }
Write-Output 'Wait for http://localhost:18082, then run run_runtime.py.'
