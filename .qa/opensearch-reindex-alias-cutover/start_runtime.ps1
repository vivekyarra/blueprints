$ErrorActionPreference = 'Stop'
$qaDir = $PSScriptRoot
foreach ($name in @('search-cutover-node', 'search-cutover-kestra')) {
    docker container inspect $name *> $null
    if ($LASTEXITCODE -eq 0) { throw "Fixture $name already exists; use a clean Docker context." }
}
New-Item -ItemType Directory -Path (Join-Path $qaDir 'plugins') -Force | Out-Null
docker run --rm -v "${qaDir}/plugins:/app/plugins" kestra/kestra:v2.0.6-no-plugins plugins install io.kestra.plugin:plugin-opensearch:1.6.2
if ($LASTEXITCODE -ne 0) { throw 'OpenSearch plugin installation failed' }
docker network create search-cutover-qa
if ($LASTEXITCODE -ne 0) { throw 'Use a fresh search-cutover-qa network' }
docker run -d --name search-cutover-node --network search-cutover-qa -p 127.0.0.1:19200:9200 -e discovery.type=single-node -e plugins.security.disabled=true -e DISABLE_INSTALL_DEMO_CONFIG=true -e 'OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m' opensearchproject/opensearch:2.19.3
if ($LASTEXITCODE -ne 0) { throw 'OpenSearch start failed' }
python (Join-Path $qaDir 'setup_runtime.py')
if ($LASTEXITCODE -ne 0) { throw 'Secret setup failed' }
docker run -d --name search-cutover-kestra --user root --network search-cutover-qa -p 127.0.0.1:18083:8080 --env-file "${qaDir}/runtime.env" -v "${qaDir}/plugins:/app/data/plugins" -v "${qaDir}/application.yaml:/etc/config/application.yaml" kestra/kestra:v2.0.6-no-plugins server local --config /etc/config/application.yaml
if ($LASTEXITCODE -ne 0) { throw 'Kestra start failed' }
Write-Output 'Wait for OpenSearch on localhost:19200 and the Kestra API on localhost:18083, then run run_runtime.py.'
