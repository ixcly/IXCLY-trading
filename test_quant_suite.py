import importlib.machinery
import unittest
import numpy as np
import pandas as pd
from pathlib import Path

module_path = Path(__file__).with_name('ixcly_quant_suite.py')
quant_suite = importlib.machinery.SourceFileLoader('ixcly_quant_suite', str(module_path)).load_module().quant_suite

class QuantSuiteTests(unittest.TestCase):
    def data(self, rising=True):
        c=np.linspace(100,120,200) if rising else np.linspace(120,100,200)
        return pd.DataFrame({'open':c-0.1,'high':c+0.5,'low':c-0.5,'close':c,'volume':np.ones(200)*100})
    def test_rising(self):
        r=quant_suite(self.data())
        self.assertEqual(r['status'],'ok')
        self.assertEqual(r['group_votes']['trend'],'LONG')
        self.assertGreaterEqual(len(r['indicators']),20)
    def test_falling(self):
        r=quant_suite(self.data(False),'SHORT')
        self.assertEqual(r['status'],'ok')
        self.assertEqual(r['group_votes']['trend'],'SHORT')
    def test_insufficient(self):
        self.assertEqual(quant_suite(self.data().iloc[:20])['status'],'insufficient_data')
    def test_invalid(self):
        d=self.data();d.loc[199,'high']=1
        self.assertEqual(quant_suite(d)['status'],'invalid_data')
    def test_no_future_data(self):
        d=self.data();r=quant_suite(d)
        d2=pd.concat([d,pd.DataFrame({'open':[999],'high':[1000],'low':[998],'close':[999],'volume':[100]})],ignore_index=True)
        self.assertEqual(quant_suite(d2.iloc[:-1])['indicators'],r['indicators'])

if __name__=='__main__': unittest.main()
