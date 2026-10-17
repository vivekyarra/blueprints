import base64
from pathlib import Path

ROOT=Path(__file__).parent
if __name__=='__main__':
    values={'OPENSEARCH_URL':'http://search-cutover-node:9200','OPENSEARCH_USER':'fixture',
            'OPENSEARCH_PASSWORD':'fixture_unused','OPENSEARCH_REINDEX_WEBHOOK_KEY':'fixture_reindex_approval'}
    (ROOT/'runtime.env').write_text('\n'.join(
        'SECRET_'+k+'='+base64.b64encode(v.encode()).decode() for k,v in values.items())+'\n')
    print('Disposable OpenSearch fixture secrets configured')
