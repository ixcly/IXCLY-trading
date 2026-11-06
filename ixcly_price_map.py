# -*- coding: utf-8 -*-
"""IXCLY Price Map: empirical, volatility-aware price scenarios, NOT price guarantees."""
from __future__ import annotations
import math


def _float(x):
    try:
        v=float(x)
        return v if math.isfinite(v) else None
    except (ValueError, TypeError, OverflowError):
        return None


def _quantile(values, q):
    values=sorted(values)
    if not values: return None
    p=(len(values)-1)*q
    lo=int(p); hi=min(lo+1,len(values)-1)
    return values[lo]+(values[hi]-values[lo])*(p-lo)


def price_map(df, side='LONG', horizons=(4, 12, 24), min_samples=40):
    """Estimate future price bands from historical close-to-close log returns.

    horizons are candle counts, not hours. Uses only prior observed candles;
    reports sample size and uncertainty, no invented success probability.
    """
    result={'status':'insufficient_data', 'method':'historical_log_return_quantiles',
            'side':str(side).upper(), 'horizons':{}, 'limitations':'Historical distribution is not a guarantee; ignores regime changes and future news.'}
    try:
        if df is None or 'close' not in df: return result
        closes=[_float(x) for x in df['close']]
        if any(v is None or v<=0 for v in closes):
            result['status']='invalid_data'; return result
        if len(closes)<min_samples+max(horizons)+1: return result
        price=closes[-1]
        for h in horizons:
            if not isinstance(h,int) or h<1 or h>len(closes)-min_samples: continue
            # Non-overlapping historical outcomes reduce misleading effective sample counts.
            returns=[math.log(closes[i+h]/closes[i]) for i in range(0,len(closes)-h,h)]
            if len(returns)<max(8,min_samples//max(1,h)):
                continue
            q10,q25,q50,q75,q90=(_quantile(returns,q) for q in (.1,.25,.5,.75,.9))
            values={'p10':q10,'p25':q25,'median':q50,'p75':q75,'p90':q90}
            band={k:round(price*math.exp(v),10) for k,v in values.items()}
            band.update({'samples':len(returns),'horizon_candles':h,'up_fraction_historical':round(sum(x>0 for x in returns)/len(returns),3),
                         'directional_move_pct':round((math.exp(q50)-1)*100,3)})
            result['horizons'][str(h)]=band
        if result['horizons']:
            result['status']='ok'
            result['reference_price']=price
    except Exception as exc:
        result['status']='calculation_error'; result['error_type']=type(exc).__name__
    return result
