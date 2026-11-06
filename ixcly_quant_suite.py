# -*- coding: utf-8 -*-
"""IXCLY Quant Suite: independent explainable indicators, no invented probabilities.
Uses only supplied OHLCV history, does not place orders or block signals.
"""
from __future__ import annotations
import math
import numpy as np


def _last(a):
    v = float(a[-1])
    return round(v, 6) if math.isfinite(v) else None


def _ema(a, period):
    alpha = 2.0 / (period + 1)
    out = np.empty(len(a), dtype=float)
    out[0] = a[0]
    for i in range(1, len(a)):
        out[i] = alpha * a[i] + (1 - alpha) * out[i-1]
    return out


def _rolling_mean(a, n):
    return np.convolve(a, np.ones(n) / n, mode='valid')


def _slope(a):
    x = np.arange(len(a), dtype=float)
    return float(np.polyfit(x, a, 1)[0])


def quant_suite(df, side='LONG', lookback=180):
    result = {'status': 'insufficient_data', 'indicators': {}, 'votes': {},
              'consensus': None, 'warnings': [], 'limitations':
              'Technical indicators are correlated; consensus is not a win probability.'}
    try:
        if df is None or not all(k in df for k in ('open','high','low','close','volume')):
            return result
        data = {k: np.asarray(df[k], dtype=float)[-max(90, min(int(lookback), 1000)):] for k in ('open','high','low','close','volume')}
        o,h,l,c,v = (data[k] for k in ('open','high','low','close','volume'))
        n = len(c)
        if n < 90 or any(len(x) != n or not np.all(np.isfinite(x)) for x in (o,h,l,c,v)):
            return result
        if np.any(c <= 0) or np.any(h < np.maximum(o,c)) or np.any(l > np.minimum(o,c)) or np.any(h < l) or np.any(v < 0):
            result['status'] = 'invalid_data'
            return result
        eps = 1e-12
        ind, votes = {}, {}
        def add(name, value, vote=None):
            if value is None or not math.isfinite(float(value)):
                return
            ind[name] = round(float(value), 6)
            if vote in (-1, 1):
                votes[name] = 'LONG' if vote == 1 else 'SHORT'
        # Trend: Donchian breakout, Aroon and Ichimoku (non-forward-shifted values).
        dh = np.max(h[-21:-1]); dl = np.min(l[-21:-1])
        add('donchian_position', (c[-1]-dl)/max(dh-dl,eps), 1 if c[-1]>dh else -1 if c[-1]<dl else None)
        hh = int(np.argmax(h[-25:])); ll = int(np.argmin(l[-25:]))
        aroon_up = (hh+1)/25*100; aroon_down=(ll+1)/25*100
        add('aroon_up', aroon_up); add('aroon_down', aroon_down)
        add('aroon_oscillator', aroon_up-aroon_down, 1 if aroon_up-aroon_down>25 else -1 if aroon_down-aroon_up>25 else None)
        tenkan=(np.max(h[-9:])+np.min(l[-9:]))/2
        kijun=(np.max(h[-26:])+np.min(l[-26:]))/2
        span_b=(np.max(h[-52:])+np.min(l[-52:]))/2
        span_a=(tenkan+kijun)/2
        add('ichimoku_tenkan',tenkan); add('ichimoku_kijun',kijun)
        add('ichimoku_cloud_top',max(span_a,span_b)); add('ichimoku_cloud_bottom',min(span_a,span_b))
        add('ichimoku_cloud_position',(c[-1]-min(span_a,span_b))/max(abs(span_a-span_b),eps),1 if c[-1]>max(span_a,span_b) and tenkan>kijun else -1 if c[-1]<min(span_a,span_b) and tenkan<kijun else None)
        # Volatility: Keltner and Bollinger bandwidth, Choppiness Index.
        prev=np.r_[c[0],c[:-1]]
        tr=np.maximum(h-l,np.maximum(np.abs(h-prev),np.abs(l-prev)))
        atr=np.mean(tr[-20:]); mid=_ema(c,20)[-1]
        add('keltner_position',(c[-1]-mid)/max(2*atr,eps),1 if c[-1]>mid+2*atr else -1 if c[-1]<mid-2*atr else None)
        std=np.std(c[-20:]); add('bollinger_bandwidth',4*std/max(np.mean(c[-20:]),eps))
        chop=100*math.log10(max(np.sum(tr[-14:])/max(np.max(h[-14:])-np.min(l[-14:]),eps),eps))/math.log10(14)
        add('choppiness_index',chop)
        # Momentum: CCI, Williams %R, Stochastic, TSI and ROC.
        typical=(h+l+c)/3
        tpmean=np.mean(typical[-20:]); mad=np.mean(np.abs(typical[-20:]-tpmean))
        cci=(typical[-1]-tpmean)/max(0.015*mad,eps)
        add('cci_20',cci,1 if cci>100 else -1 if cci < -100 else None)
        will=-100*(np.max(h[-14:])-c[-1])/max(np.max(h[-14:])-np.min(l[-14:]),eps)
        add('williams_r_14',will,1 if will>-20 else -1 if will<-80 else None)
        stoch=100*(c[-1]-np.min(l[-14:]))/max(np.max(h[-14:])-np.min(l[-14:]),eps)
        add('stochastic_k_14',stoch,1 if stoch>80 else -1 if stoch<20 else None)
        momentum=np.diff(c)
        tsi=100*_ema(_ema(momentum,25),13)[-1]/max(_ema(_ema(np.abs(momentum),25),13)[-1],eps)
        add('true_strength_index',tsi,1 if tsi>10 else -1 if tsi<-10 else None)
        roc=100*(c[-1]/c[-13]-1)
        add('rate_of_change_12',roc,1 if roc>1 else -1 if roc<-1 else None)
        # Money flow: Chaikin money flow and accumulation/distribution slope.
        mult=((c-l)-(h-c))/np.maximum(h-l,eps)
        mf=mult*v
        cmf=np.sum(mf[-20:])/max(np.sum(v[-20:]),eps)
        add('chaikin_money_flow_20',cmf,1 if cmf>0.1 else -1 if cmf<-0.1 else None)
        adl=np.cumsum(mf)
        adl_slope=_slope(adl[-20:])/max(np.mean(v[-20:]),eps)
        add('adl_slope_normalized',adl_slope,1 if adl_slope>0.1 else -1 if adl_slope<-0.1 else None)
        # Price path: efficiency ratio and realized downside/upside volatility.
        dif=np.diff(c[-21:]); efficiency=abs(c[-1]-c[-21])/max(np.sum(np.abs(dif)),eps)
        add('kaufman_efficiency_ratio',efficiency)
        returns=np.diff(np.log(c[-61:]))
        add('downside_volatility',math.sqrt(float(np.mean(np.minimum(returns,0)**2))))
        add('upside_volatility',math.sqrt(float(np.mean(np.maximum(returns,0)**2))))
        # Independent category consensus: one category = one vote to avoid correlated stacking.
        groups={'trend':['donchian_position','aroon_oscillator','ichimoku_cloud_position'],
                'volatility_breakout':['keltner_position'],
                'momentum':['cci_20','williams_r_14','stochastic_k_14','true_strength_index','rate_of_change_12'],
                'money_flow':['chaikin_money_flow_20','adl_slope_normalized']}
        group_votes={}
        for group,names in groups.items():
            long=sum(votes.get(k)=='LONG' for k in names)
            short=sum(votes.get(k)=='SHORT' for k in names)
            group_votes[group]='LONG' if long>short else 'SHORT' if short>long else 'NEUTRAL'
        target=str(side).upper()
        active=[x for x in group_votes.values() if x!='NEUTRAL']
        result.update(status='ok', indicators=ind, votes=votes, group_votes=group_votes,
                      consensus=round(sum(x==target for x in active)/len(active),3) if active else None,
                      active_groups=len(active), bars=n)
        if chop>61.8: result['warnings'].append('choppy_market')
        if efficiency<0.2: result['warnings'].append('inefficient_price_path')
        if np.sum(v[-20:])<=0: result['warnings'].append('no_volume')
        return result
    except (ValueError, TypeError, IndexError, FloatingPointError, OverflowError, KeyError) as exc:
        result['status']='calculation_error'; result['warnings'].append(type(exc).__name__)
        return result
