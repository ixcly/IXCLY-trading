import unittest
import numpy as np
import pandas as pd
from ixcly_precision import candle_diagnostics, execution_geometry, precision_report, position_risk_plan, robust_data_health, stop_stress_test

class PrecisionTests(unittest.TestCase):
    def setUp(self):
        close = np.linspace(100, 120, 64)
        self.df = pd.DataFrame({'open': close - .1, 'high': close + .4,
                                'low': close - .5, 'close': close,
                                'volume': np.full(64, 1000.)})

    def test_candles(self):
        r = candle_diagnostics(self.df, 'LONG')
        self.assertEqual(r['status'], 'ok')
        self.assertGreater(r['directional_efficiency'], .95)

    def test_insufficient(self):
        self.assertEqual(candle_diagnostics(self.df.head(4), 'SHORT')['status'], 'insufficient_data')

    def test_long_geometry(self):
        r = execution_geometry('LONG', 100, 98, [104])
        self.assertEqual(r['status'], 'ok')
        self.assertEqual(r['targets'][0]['gross_rr'], 2)
        self.assertLess(r['targets'][0]['net_rr'], 2)

    def test_short_geometry(self):
        r = execution_geometry('SHORT', 100, 102, [96])
        self.assertEqual(r['targets'][0]['gross_rr'], 2)

    def test_wrong_stop(self):
        self.assertEqual(execution_geometry('SHORT', 100, 98, [95])['status'], 'invalid_stop')

    def test_risk_plan(self):
        r = position_risk_plan(1000, 100, 98)
        self.assertEqual(r['status'], 'ok')
        self.assertLessEqual(r['notional'], 250)
        self.assertTrue(r['paper_only'])

    def test_health(self):
        self.assertEqual(robust_data_health(self.df)['status'], 'ok')

    def test_stress(self):
        r = stop_stress_test('LONG', 100, 98, 1)
        self.assertEqual(r['stress_cases'][-1]['hypothetical_loss_pct'], 4)

    def test_report(self):
        r = precision_report(self.df, {'side':'LONG','entry':120,'sls':[117], 'tps':[126]})
        self.assertTrue(r['advisory_only'])
        self.assertFalse(r['is_probability'])

if __name__ == '__main__':
    unittest.main()
