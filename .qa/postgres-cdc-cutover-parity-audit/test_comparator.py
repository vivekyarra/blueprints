"""Regression checks against functions extracted from the submitted YAML, never copies."""
import ast
import random
import unittest
from pathlib import Path

import yaml

FLOW = next(p for parent in Path(__file__).resolve().parents for p in
    (parent / 'flows/postgres-cdc-cutover-parity-audit.yaml',
     parent / 'blueprints/flows/postgres-cdc-cutover-parity-audit.yaml') if p.exists())
flow = yaml.safe_load(FLOW.read_text())
script = next(t['script'] for t in flow['tasks'] if t['id'] == 'compare_rows')
tree = ast.parse(script)
functions = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in ('compare', 'rows')]
ns = {'FIELDS': ('email', 'full_name', 'plan', 'notes', 'updated_at')}
exec(compile(ast.Module(body=functions, type_ignores=[]), str(FLOW), 'exec'), ns)

def row(i, value=None):
    return (i, value, 'name', 'free', None, None)

def compare(a, b, limit=10000, samples=20):
    return ns['compare'](ns['rows'](a), ns['rows'](b), limit, samples)

class ComparatorTest(unittest.TestCase):
    def test_equal(self):
        counts, examples = compare([row(-1), row(2)], [row(-1), row(2)])
        self.assertEqual(counts['matched'], 2)
        self.assertFalse(examples)

    def test_missing(self):
        self.assertEqual(compare([row(1)], [])[0]['missing'], 1)

    def test_extra(self):
        self.assertEqual(compare([], [row(1)])[0]['extra'], 1)

    def test_changed(self):
        counts, examples = compare([row(1, 'secret-a')], [row(1, 'secret-b')])
        self.assertEqual(counts['changed'], 1)
        self.assertEqual(examples, [{'id': 1, 'kind': 'CHANGED', 'fields': ['email']}])
        self.assertNotIn('secret', repr(examples))

    def test_null_and_unicode(self):
        self.assertEqual(compare([row(1, None)], [row(1, '')])[0]['changed'], 1)
        self.assertEqual(compare([row(1, 'Grüße 東京')], [row(1, 'Grüße 東京')])[0]['matched'], 1)

    def test_empty(self):
        self.assertTrue(all(n == 0 for n in compare([], [])[0].values()))

    def test_sample_cap(self):
        counts, examples = compare([row(i) for i in range(1000)], [], samples=3)
        self.assertEqual(counts['missing'], 1000)
        self.assertEqual(len(examples), 3)

    def test_row_limit(self):
        for a, b in [([row(1), row(2)], []), ([], [row(1), row(2)]),
                     ([row(1), row(2)], [row(1), row(2)])]:
            with self.subTest(a=a, b=b), self.assertRaisesRegex(ValueError, 'row limit'):
                compare(a, b, limit=1)

    def test_exact_limit(self):
        self.assertEqual(compare([row(1)], [row(1)], limit=1)[0]['matched'], 1)

    def test_duplicate_and_unordered_keys(self):
        for keys in ([1, 1], [2, 1]):
            for side in (0, 1):
                args = [[], []]
                args[side] = [row(i) for i in keys]
                with self.subTest(keys=keys, side=side), self.assertRaisesRegex(ValueError, 'unique'):
                    compare(*args)

    def test_invalid_keys(self):
        for key in (None, '1', True, 1.0):
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, 'integer'):
                compare([row(key)], [])

    def test_wrong_shape(self):
        with self.assertRaisesRegex(ValueError, 'six columns'):
            compare([(1, 'x')], [])

    def test_lazy_row_limit(self):
        def infinite():
            for i in range(20):
                yield row(i)
            raise AssertionError('iterator was eagerly consumed')
        with self.assertRaisesRegex(ValueError, 'row limit'):
            compare(infinite(), [], limit=5)

    def test_large_integer_keys(self):
        self.assertEqual(compare([row(2**63-1)], [row(2**63-1)])[0]['matched'], 1)

    def test_randomized_oracle(self):
        rng = random.Random(7321)
        for case in range(500):
            a = {i: rng.choice([None, '', '東京', 'a', 'b'])
                 for i in rng.sample(range(200), rng.randrange(120))}
            b = {i: rng.choice([None, '', '東京', 'a', 'b'])
                 for i in rng.sample(range(200), rng.randrange(120))}
            counts, examples = compare([row(i, v) for i, v in sorted(a.items())],
                                       [row(i, v) for i, v in sorted(b.items())], samples=7)
            common = a.keys() & b.keys()
            with self.subTest(case=case):
                self.assertEqual(counts, dict(source_rows=len(a), replica_rows=len(b),
                    missing=len(a.keys()-b.keys()), extra=len(b.keys()-a.keys()),
                    matched=sum(a[i] == b[i] for i in common),
                    changed=sum(a[i] != b[i] for i in common)))
                self.assertLessEqual(len(examples), 7)

if __name__ == '__main__':
    unittest.main(verbosity=2)
