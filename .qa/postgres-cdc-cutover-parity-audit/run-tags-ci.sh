set -eu
python -m pip install --quiet PyYAML==6.0.3
bash /qa/validate-tags-ci.sh > /qa/catalog-tags-ci.log 2>&1
tail -n 3 /qa/catalog-tags-ci.log
