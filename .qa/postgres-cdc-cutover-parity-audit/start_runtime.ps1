$ErrorActionPreference = 'Stop'
$qaDir = $PSScriptRoot
foreach ($name in @('parity-source', 'parity-replica', 'parity-kestra')) {
    docker container inspect $name *> $null
    if ($LASTEXITCODE -eq 0) { throw "Container $name already exists. Use a clean Docker context for this disposable fixture." }
}
New-Item -ItemType Directory -Path (Join-Path $qaDir 'plugins') -Force | Out-Null
docker run --rm -v "${qaDir}/plugins:/app/plugins" kestra/kestra:v2.0.6-no-plugins plugins install io.kestra.plugin:plugin-jdbc-postgres:2.4.0 io.kestra.plugin:plugin-script-python:1.17.0 io.kestra.plugin:plugin-debezium-postgres:3.0.1
if ($LASTEXITCODE -ne 0) { throw 'Plugin installation failed' }
foreach ($name in @('parity-source', 'parity-replica')) {
    docker run -d --name $name -e POSTGRES_PASSWORD=parity_fixture_admin postgres:16
    if ($LASTEXITCODE -ne 0) { throw "Cannot create $name" }
    $ready = $false
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        docker exec $name pg_isready -U postgres *> $null
        if ($LASTEXITCODE -eq 0) { $ready = $true; break }
        Start-Sleep -Seconds 1
    }
    if (-not $ready) { throw "$name did not become ready" }
}
python (Join-Path $qaDir 'setup_runtime.py')
if ($LASTEXITCODE -ne 0) { throw 'Fixture schema setup failed' }
@'
datasources:
  h2:
    url: jdbc:h2:file:/app/data/db;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE
    username: sa
    password: ""
    driver-class-name: org.h2.Driver
kestra:
  server:
    basic-auth:
      username: admin@kestra.local
      password: Kestra-local-1
  repository:
    type: h2
  queue:
    type: h2
  storage:
    type: local
    local:
      base-path: /app/storage
'@ | Set-Content -LiteralPath (Join-Path $qaDir 'application.yaml')
docker run -d --name parity-kestra --user root -p 127.0.0.1:18080:8080 --env-file "${qaDir}/runtime.env" -v "${qaDir}/plugins:/app/data/plugins" -v "${qaDir}/application.yaml:/etc/config/application.yaml" -v /var/run/docker.sock:/var/run/docker.sock kestra/kestra:v2.0.6-no-plugins server local --config /etc/config/application.yaml
if ($LASTEXITCODE -ne 0) { throw 'Kestra start failed' }
Write-Output 'Fixture started. Wait for the Kestra API on localhost:18080, then run run_runtime.py, run_identity.py, run_cdc.py, and verify_readonly.py.'
