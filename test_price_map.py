import unittest
import pandas as pd
from ixcly_price_map import price_map

class PriceMapTests(unittest.TestCase):
    def test_insufficient(self):
        self.assertEqual(price_map(pd.DataFrame({'close':[1,2,3]}))['status'], 'insufficient_data')
    def test_invalid(self):
        self.assertEqual(price_map(pd.DataFrame({'close':[0]*200}))['status'], 'invalid_data')
    def test_flat(self):
        result=price_map(pd.DataFrame({'close':[100.0]*300}))
        self.assertEqual(result['status'],'ok')
        self.assertEqual(result['horizons']['4']['median'],100.0)
    def test_increasing(self):
        result=price_map(pd.DataFrame({'close':[100*(1.001**i) for i in range(300)]}))
        self.assertEqual(result['status'],'ok')
        self.assertGreater(result['horizons']['4']['median'],result['reference_price'])
    def test_quantile_order(self):
        result=price_map(pd.DataFrame({'close':[100+0.5*i+3*((i%7)-3) for i in range(300)]}))
        x=result['horizons']['4']
        self.assertLessEqual(x['p10'],x['median'])
        self.assertLessEqual(x['median'],x['p90'])

if __name__=='__main__': unittest.main()
