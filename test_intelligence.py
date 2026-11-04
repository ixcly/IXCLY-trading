import math
import unittest
import pandas as pd
import numpy as np
import ixcly_intelligence as ix


class IntelligenceTests(unittest.TestCase):
    def setUp(self):
        n = 100
        close = np.linspace(100, 130, n)
        self.df = pd.DataFrame({
            'open': close - 0.3,
            'high': close + 1.0,
            'low': close - 1.0,
            'close': close,
            'volume': np.linspace(1000, 1800, n),
            'atr': np.full(n, 1.5),
            'rsi': np.linspace(45, 65, n),
            'adx': np.full(n, 28.0),
            'plus_di': np.full(n, 30.0),
            'minus_di': np.full(n, 15.0),
            'vwap': close - 0.5,
            'obv': np.linspace(10000, 20000, n),
            'obv_ema': np.linspace(9000, 19000, n),
            'rel_vol': np.full(n, 1.4),
            'div_bull': np.zeros(n, dtype=bool),
            'div_bear': np.zeros(n, dtype=bool),
            'squeeze': np.zeros(n, dtype=bool),
        })

    def test_indicator_snapshot(self):
        out = ix.indicator_snapshot(self.df)
        self.assertEqual(out['vwap_side'], 'above')
        self.assertEqual(out['obv_trend'], 'inflow')
        self.assertGreater(out['adx'], 0)

    def test_breakout_assessment_returns_state(self):
        out = ix.breakout_assessment(self.df)
        self.assertIn(out['state'], ('range', 'breakout_up', 'breakout_down', 'fake_up', 'fake_down', 'unknown'))

    def test_confirmed_and_fake_breakouts(self):
        df = self.df.copy()
        df.loc[df.index[-1], ['open', 'high', 'low', 'close', 'rel_vol', 'atr']] = [130, 134, 129, 133.5, 2.0, 1.5]
        self.assertEqual(ix.breakout_assessment(df)['state'], 'breakout_up')
        df.loc[df.index[-1], ['open', 'high', 'low', 'close', 'rel_vol', 'atr']] = [130, 134, 129, 130.2, 2.0, 1.5]
        self.assertEqual(ix.breakout_assessment(df)['state'], 'fake_up')

    def test_anti_fomo_only_directional_extension(self):
        out = ix.anti_fomo_assessment(self.df, 'LONG')
        self.assertFalse(out['blocked'])
        self.assertGreaterEqual(out['penalty'], 0)
        stretched = self.df.copy()
        stretched.loc[stretched.index[-1], ['close', 'ema20', 'atr', 'rsi']] = [140, 130, 1, 90]
        self.assertTrue(ix.anti_fomo_assessment(stretched, 'LONG')['blocked'])

    def test_news_relevance_is_coin_scoped(self):
        news = {'ts': 1000, 'title_ru': 'Инцидент с SOL', 'summary': 'SOL protocol issue',
                'coins': 'SOL', 'severity': 'high'}
        self.assertTrue(ix.news_risk_assessment(news, 'SOL', now=1100)['relevant'])
        self.assertFalse(ix.news_risk_assessment(news, 'ADA', now=1100)['relevant'])

    def test_market_panic_levels(self):
        self.assertEqual(ix.market_panic_assessment(0.2, 1.0)['level'], 'normal')
        self.assertIn(ix.market_panic_assessment(3.2, 3.0)['level'], ('high', 'critical'))

    def test_rank_and_memory(self):
        tf = {'4h': {'long_prob': 70, 'adx': 28, 'qv24': 5e7, 'df': self.df},
              '1d': {'long_prob': 68}}
        self.assertGreater(ix.scanner_rank_score(tf, 70), 50)
        hist = [{'status': 'win', 'r': 1.5, 'ts': 3, 'market_regime': {'name': 'uptrend'}},
                {'status': 'loss', 'r': -1, 'ts': 2, 'market_regime': {'name': 'range'}}]
        mem = ix.signal_memory_summary(hist)
        self.assertEqual(mem['trades'], 2)
        self.assertEqual(mem['win_rate'], 50.0)

    def test_adaptive_threshold_waits_for_sample(self):
        hist = [{'status': 'loss', 'r': -1, 'ts': i} for i in range(8)]
        self.assertEqual(ix.adaptive_scanner_profile(hist)['adjustment'], 0)
        hist = [{'status': 'loss', 'r': -1, 'ts': i, 'market_regime': {'name': 'range'}} for i in range(8)]
        self.assertEqual(ix.adaptive_scanner_profile(hist, regime='range')['adjustment'], 6)
        hist = [{'status': 'loss', 'r': -1, 'ts': i} for i in range(15)]
        self.assertEqual(ix.adaptive_scanner_profile(hist)['adjustment'], 6)

    def test_breakeven_requires_full_2_5_percent(self):
        self.assertFalse(ix.breakeven_trigger_reached(0.0, 2.5))
        self.assertFalse(ix.breakeven_trigger_reached(2.49, 2.5))
        self.assertTrue(ix.breakeven_trigger_reached(2.5, 2.5))
        self.assertTrue(ix.breakeven_trigger_reached(3.1, 2.5))

    def test_close_warning_requires_all_four_timeframes(self):
        adverse_long = {"15m": 25, "1h": 28, "4h": 30, "1d": 32}
        self.assertTrue(ix.close_warning_confirmed(adverse_long, "LONG"))
        adverse_short = {"15m": 75, "1h": 72, "4h": 70, "1d": 68}
        self.assertTrue(ix.close_warning_confirmed(adverse_short, "SHORT"))
        mixed = {"15m": 25, "1h": 28, "4h": 30, "1d": 55}
        self.assertFalse(ix.close_warning_confirmed(mixed, "LONG"))
        self.assertFalse(ix.close_warning_confirmed({"15m": 20, "1h": 20}, "LONG"))

    def test_forecast_scenarios_are_atr_based(self):
        tf = {"4h": {"price": 100, "atr": 2, "long_prob": 70, "df": self.df},
              "1d": {"long_prob": 65}}
        out = ix.forecast_scenarios(tf)
        self.assertEqual(out["status"], "ok")
        self.assertEqual(out["direction"], "LONG")
        self.assertGreater(out["bull_target"], out["price"])
        self.assertLess(out["bear_target"], out["price"])

    def test_early_movement_is_watch_only(self):
        out = ix.early_movement_assessment(self.df, oi_change_pct=1.2, book_imbalance=.7, funding_rate=.0001)
        self.assertIn(out["state"], ("watch", "quiet"))
        self.assertIn("score", out)
        self.assertIn("message", out)

    def test_liquidation_risk_is_proxy_not_price_map(self):
        out = ix.liquidation_risk_assessment(.0008, 1.0, -0.8)
        self.assertGreater(out["long_crowding_risk"], 0)
        self.assertIn("не реальные уровни", out["note"])

    def test_strategy_stats_requires_minimum_sample(self):
        hist = [{"status":"win", "type":"short", "r":1.0}, {"status":"loss", "type":"short", "r":-1.0}]
        out = ix.strategy_performance(hist)
        self.assertEqual(out[0]["trades"], 2)
        self.assertFalse(out[0]["sample_ok"])

    def test_self_audit_small_sample_warning(self):
        out = ix.self_audit_summary([{"status":"win", "r":1.0}])
        self.assertEqual(out["trades"], 1)
        self.assertTrue(any("мала" in x for x in out["findings"]))

    def test_atr_profiles(self):
        self.assertGreater(ix.atr_stop_multiplier(20, 4.5), ix.atr_stop_multiplier(20, 1.0))
        self.assertGreater(ix.atr_target_multipliers(35, 1.0)[-1], ix.atr_target_multipliers(15, 4.5)[-1])



class QuantumIntelligenceTests(unittest.TestCase):
    def setUp(self):
        import numpy as np
        import pandas as pd
        rng = np.random.default_rng(7)
        close = 100 * np.cumprod(1 + rng.normal(0.0005, 0.008, 260))
        self.df = pd.DataFrame({'open': close, 'high': close*1.01, 'low': close*.99,
                                'close': close, 'volume': rng.uniform(100, 500, len(close)),
                                'timestamp': np.arange(len(close))*900000 + 1700000000000})

    def test_quant_models_have_bounded_outputs(self):
        from ixcly_intelligence import (regime_switching_model, kalman_trend_filter, hurst_exponent,
            entropy_detector, distribution_shift_detector, extreme_value_risk_model,
            mean_reversion_probability, volatility_forecast, data_integrity_guard)
        self.assertEqual(data_integrity_guard(self.df)['status'], 'good')
        self.assertIn(regime_switching_model(self.df)['regime'], ('panic','high_volatility','volatility_compression','strong_trend','weak_trend','range'))
        self.assertIn(kalman_trend_filter(self.df)['direction'], ('LONG','SHORT','FLAT'))
        self.assertGreaterEqual(hurst_exponent(self.df)['hurst'], 0)
        self.assertGreaterEqual(entropy_detector(self.df)['entropy'], 0)
        self.assertIn(distribution_shift_detector(self.df)['shift'], (True, False))
        self.assertGreaterEqual(extreme_value_risk_model(self.df)['tail_mean_pct'], 0)
        self.assertIn(mean_reversion_probability(self.df)['status'], ('ok','insufficient_data'))
        self.assertGreater(volatility_forecast(self.df)['ewma_vol_pct'], 0)

    def test_probability_and_execution_guards(self):
        from ixcly_intelligence import (bayesian_signal_probability, sequential_evidence_accumulator,
            chasing_protection, partial_fill_controller, spread_intelligence, emergency_risk_lock)
        hist=[{'status':'closed','closed_at':i+1,'r':1} for i in range(15)]
        p=bayesian_signal_probability({'long':.8,'short':.2,'independent_strength':2},hist)
        self.assertGreater(p['long_pct'], p['short_pct'])
        self.assertEqual(sequential_evidence_accumulator([{'name':'a','log_lr':1}])['decision'],'WAIT')
        self.assertFalse(chasing_protection(110,100,5,'LONG')['allow'])
        self.assertEqual(partial_fill_controller(10,4)['status'],'partial')
        self.assertGreater(spread_intelligence(99,101)['spread_pct'],0)
        self.assertTrue(emergency_risk_lock({'status':'poor'})['locked'])

    def test_market_microstructure_and_portfolio_guards(self):
        from ixcly_intelligence import (volume_price_divergence_matrix, fractal_market_analysis,
            volatility_compression_scanner, correlation_clusters, risk_budget_allocator,
            liquidation_cascade_forecast, market_breadth_engine, cross_exchange_watch,
            trade_opportunity_ranking, out_of_sample_testing, strategy_attribution,
            walk_forward_validation, probability_calibration_report, strategy_lab_report,
            funding_convergence, breakout_failure_probability, signal_decay_assessment)
        self.assertEqual(volume_price_divergence_matrix(self.df)['status'], 'ok')
        self.assertIn(fractal_market_analysis({'15m':{'df':self.df},'4h':{'df':self.df}})['status'], ('ok','insufficient_data'))
        self.assertIn(volatility_compression_scanner(self.df)['compressed'], (True,False))
        self.assertEqual(len(correlation_clusters({'A':[i/100 for i in range(40)],'B':[i/50 for i in range(40)]})['clusters']), 1)
        self.assertGreaterEqual(sum(x['risk_pct'] for x in risk_budget_allocator(1000,[{'coin':'A','quality':80},{'coin':'B','quality':40}])['allocations']),0)
        self.assertIn(liquidation_cascade_forecast(funding=.001,oi_change=5)['level'], ('low','medium','high'))
        self.assertEqual(market_breadth_engine({'A':1,'B':-1})['status'],'ok')
        self.assertTrue(cross_exchange_watch({'A':100,'B':102},1)['opportunity'])
        self.assertEqual(trade_opportunity_ranking([{'coin':'A','edge':5,'risk':1,'data_ok':True}])['ranking'][0]['coin'],'A')
        hist=[{'status':'closed','closed_at':i+1,'r':1,'regime':'trend'} for i in range(40)]
        self.assertEqual(out_of_sample_testing(hist)['status'],'ok')
        self.assertEqual(strategy_attribution(hist)['status'],'ok')
        self.assertEqual(walk_forward_validation(hist)['status'],'ok')
        calibrated=[dict(x,predicted_probability=.7) for x in hist]
        self.assertEqual(probability_calibration_report(calibrated)['status'],'ok')
        self.assertEqual(strategy_lab_report(calibrated)['status'],'ok')
        self.assertEqual(funding_convergence([.001,.0008,.0004])['state'],'crowding_easing')
        self.assertIn(breakout_failure_probability(self.df)['status'], ('ok','insufficient_data'))
        self.assertFalse(signal_decay_assessment({'ts':1,'entry':100},100,now=100000,ttl_seconds=100)['valid'])

    def test_crypto_news_editorial_filter(self):
        from ixcly_news import assess_crypto_news
        crypto=assess_crypto_news("Binance announces new BTC spot ETF custody product", "Bitcoin BTC exchange custody details "*30, "coindesk.com")
        noise=assess_crypto_news("Celebrity launches new AI chatbot", "A celebrity announced an artificial intelligence chatbot."*5, "techcrunch.com")
        self.assertTrue(crypto['publish'])
        self.assertFalse(noise['publish'])

    def test_paper_execution_conservative_rules(self):
        from ixcly_paper import simulate_order_execution, conservative_candle_exit
        fill=simulate_order_execution('LONG',10,100,spread_pct=.1,depth_notional=100000,latency_ms=120)
        self.assertEqual(fill['status'],'filled')
        self.assertGreater(fill['execution_price'],100)
        partial=simulate_order_execution('LONG',10,100,available_notional=1000)
        self.assertEqual(partial['status'],'partial')
        both=conservative_candle_exit('LONG',95,105,106,94)
        self.assertEqual(both['status'],'stop')
        self.assertTrue(both['ambiguous'])

    def test_portfolio_risk_and_quant_stack(self):
        from ixcly_intelligence import (portfolio_var_es, drawdown_guard, daily_loss_guard,
            quant_intelligence_stack, monte_carlo_trade_simulator)
        self.assertEqual(portfolio_var_es([[.01],[-.02]]*20)['status'],'ok')
        self.assertTrue(drawdown_guard([100,110,90])['locked'])
        self.assertTrue(daily_loss_guard([-4,-2],100,5)['locked'])
        tf={'4h':{'df':self.df,'price':float(self.df.close.iloc[-1]),'atr':1.2},
            '1h':{'df':self.df},'15m':{'df':self.df},'1d':{'df':self.df}}
        q=quant_intelligence_stack(tf,'LONG',float(self.df.close.iloc[-1]),float(self.df.close.iloc[-1])*.98,
                                  [float(self.df.close.iloc[-1])*1.02],history=[])
        self.assertIn(q['risk_state'],('GREEN','YELLOW','RED'))
        self.assertEqual(q['monte_carlo']['status'],'deferred_for_resource_budget')
        mc=monte_carlo_trade_simulator(self.df,100,98,[102],'LONG',simulations=250)
        self.assertEqual(mc['status'],'ok')
        self.assertIsNotNone(mc['tp_before_sl_pct'])


    def test_signed_cvd_uses_aggressor_side(self):
        rows = [
            {"p": "100", "q": "2", "m": False, "T": 1000},
            {"p": "100", "q": "1", "m": True, "T": 2000},
        ]
        out = ix.signed_cvd_from_agg_trades(rows)
        self.assertEqual(out["status"], "ok")
        self.assertEqual(out["delta_quote"], 100.0)
        self.assertAlmostEqual(out["delta_pct"], 100/300*100, places=2)

    def test_macro_event_shield_requires_trusted_feed_status(self):
        now = 1_800_000_000
        self.assertEqual(ix.macro_event_shield([], now=now)["status"], "no_event_feed")
        self.assertEqual(ix.macro_event_shield([], now=now, feed_status="ok")["status"], "ok")
        event = {"ts": now + 300, "impact": "high", "event": "CPI"}
        out = ix.macro_event_shield([event], now=now, window_minutes=30, feed_status="ok")
        self.assertTrue(out["blocked"])

    def test_parameter_sensitivity_uses_only_closed_outcomes(self):
        history = [{"closed_at": i + 1, "strategy_score": 55 + i, "r": 0.5 if i % 3 else -0.2}
                   for i in range(30)]
        history.append({"strategy_score": 99, "r": 100})
        out = ix.parameter_sensitivity_from_history(history, min_sample=5)
        self.assertEqual(out["status"], "ok")
        self.assertEqual(out["observations"], 30)
        self.assertIn("variants", out)


    def test_macro_normalization_skips_stale_and_date_only_events(self):
        raw = [
            {"impact":"high","country":"US","category":"economic-indicators","name":"CPI",
             "release_at_utc":"2026-11-05T13:30:00Z","data_stale":False},
            {"impact":"high","country":"US","category":"economic-indicators","name":"Unknown time",
             "release_at_utc":None,"date":"2026-11-05"},
            {"impact":"high","country":"US","category":"economic-indicators","name":"Stale",
             "release_at_utc":"2026-11-05T13:30:00Z","data_stale":True},
            {"impact":"low","country":"US","category":"economic-indicators","name":"Low impact",
             "release_at_utc":"2026-11-05T13:30:00Z"},
        ]
        out = ix.normalize_macro_calendar_events(raw, source="FinanceCalendar")
        self.assertEqual(len(out["events"]), 1)
        self.assertEqual(out["untimed_rows"], 1)
        self.assertEqual(out["stale_rows"], 1)

if __name__ == '__main__':
    unittest.main()

class AdaptiveQuant52Tests(unittest.TestCase):
    def test_conflict_detector_ignores_unknown_and_counts_votes(self):
        out = ix.signal_conflict_detector('LONG', {'cvd':'SHORT','trend':'LONG','unknown':None})
        self.assertEqual(out['status'], 'ok')
        self.assertEqual(out['disagree'], 1)

    def test_multi_timeframe_consensus_reports_missing(self):
        out = ix.multi_timeframe_consensus({'15m':{'long_prob':80},'4h':{'long_prob':30}}, 'LONG')
        self.assertEqual(out['status'], 'ok')
        self.assertIn('1h', out['missing'])
        self.assertTrue(out['conflict'])

    def test_dynamic_entry_optimizer_has_zone_and_wait_state(self):
        out = ix.dynamic_entry_optimizer(100, 2, 'LONG')
        self.assertEqual(out['status'], 'ok')
        self.assertIn('allow_now', out)
        self.assertIn('zone_low', out)

    def test_dynamic_tp_sl_is_directional(self):
        long = ix.dynamic_tp_sl_optimizer(100, 2, 'LONG')
        short = ix.dynamic_tp_sl_optimizer(100, 2, 'SHORT')
        self.assertLess(long['stop'], 100)
        self.assertGreater(long['target_1'], 100)
        self.assertGreater(short['stop'], 100)
        self.assertLess(short['target_1'], 100)

    def test_strategy_tournament_requires_minimum_sample(self):
        rows=[{'strategy_type':'Trend Following','r':1.0} for _ in range(25)]
        out=ix.strategy_tournament(rows)
        self.assertEqual(out['leader'], 'Trend Following')

    def test_funding_anomaly_requires_history(self):
        self.assertEqual(ix.funding_anomaly_detector([.001])['status'], 'insufficient_data')
        out=ix.funding_anomaly_detector([.001,.001,.001,.02])
        self.assertTrue(out['anomaly'])

    def test_liquidation_clusters_never_infer_without_observed_prints(self):
        out=ix.liquidation_cluster_map([{'price':100,'notional':50}], 100)
        self.assertEqual(out['status'], 'insufficient_data')

    def test_risk_of_ruin_is_bounded(self):
        out=ix.risk_of_ruin_simulator(55, 1.5, 1.0, simulations=100, trades=50)
        self.assertGreaterEqual(out['risk_of_ruin_pct'], 0)
        self.assertLessEqual(out['risk_of_ruin_pct'], 100)

    def test_adaptive_quant_engine_returns_full_module_set(self):
        close=np.linspace(100,130,100)
        df=pd.DataFrame({'open':close-.2,'high':close+1,'low':close-1,'close':close,'volume':np.linspace(1000,1800,100),'atr':np.full(100,1.5),'adx':np.full(100,25.)})
        tf={k:{'df':df.copy(),'long_prob':65,'price':130.0,'atr':1.5,'sr':{}} for k in ('15m','1h','4h','1d')}
        out=ix.adaptive_quant_engine(tf, 'LONG', 130.0, 1.5, history=[{'r':.5,'strategy_type':'Trend Following'} for _ in range(25)])
        self.assertEqual(out['status'], 'ok')
        for key in ('market_regime_ai','multi_timeframe_consensus','signal_conflicts','signal_confidence_breakdown',
                    'adaptive_strategy','dynamic_entry_optimizer','signal_expiration','dynamic_tp_sl',
                    'strategy_tournament','funding_anomaly','oi_price_divergence','spoofing_suspicion',
                    'liquidation_clusters','portfolio_exposure','risk_of_ruin','system_health'):
            self.assertIn(key, out)
