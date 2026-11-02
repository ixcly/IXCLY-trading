import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from ixcly_brain import (
    regime_transition_assessment, signal_fragility, evidence_conflict_map, score_calibration
)

class Frame:
    def __init__(self, data): self.data = data
    def __getitem__(self, key):
        class Col:
            def __init__(self, values): self.values = values
            def tolist(self): return self.values
        return Col(self.data[key])

def frame(n=40, breakout=False):
    close=[]; high=[]; low=[]; volume=[]
    px=100.0
    for i in range(n):
        px += (0.15 if i < n-8 else (1.2 if breakout else 0.1))
        close.append(px); high.append(px+0.5); low.append(px-0.5)
        volume.append(100 if i < n-8 else (180 if breakout else 100))
    return Frame({'close':close,'high':high,'low':low,'volume':volume})

def test_regime_transition_detects_expansion():
    x=regime_transition_assessment(frame(breakout=True))
    assert x['status']=='ok'
    assert x['state'] in ('volatility_expansion','momentum_acceleration')

def test_regime_transition_requires_history():
    x=regime_transition_assessment(frame(n=20))
    assert x['status']=='insufficient_data'

def test_signal_fragility_leave_one_out():
    dna={'score':75,'factors':[
        {'name':'A','value':100,'weight':.8},
        {'name':'B','value':40,'weight':.2}]}
    x=signal_fragility(dna)
    assert x['status']=='ok' and x['dominant_dependency']=='A'
    assert x['level'] in ('medium','high')

def test_evidence_conflict_map():
    x=evidence_conflict_map('LONG', {'orderbook_bid_share':.35,'funding':.001,'news_bias':-1.0}, frame())
    assert x['status']=='ok'
    assert x['state']=='conflict'
    assert x['conflict_pct'] is not None

def test_score_calibration_uses_only_scored_closed():
    hist=[]
    for i in range(10):
        hist.append({'status':'win' if i<6 else 'loss','r':1 if i<6 else -1,
                     'signal_dna':{'score':82}})
    hist.append({'status':'active','r':0,'signal_dna':{'score':90}})
    x=score_calibration(hist,8)
    assert x['scored_closed_trades']==10
    assert x['bands'][0]['band']=='80–100'
    assert x['bands'][0]['win_rate']==60.0
    assert x['bands'][0]['sample_ok'] is True
