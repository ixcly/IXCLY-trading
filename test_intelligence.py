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


if __name__ == '__main__':
    unittest.main()

class QuantIntelligenceTests(unittest.TestCase):
    def test_directional_confidence_penalizes_timeframe_disagreement(self):
        aligned = {k: {'long_prob': v} for k, v in {'1d': 75, '4h': 72, '1h': 70, '15m': 68}.items()}
        mixed = {k: {'long_prob': v} for k, v in {'1d': 75, '4h': 72, '1h': 30, '15m': 25}.items()}
        a = ix.directional_confidence(aligned, 'LONG')
        b = ix.directional_confidence(mixed, 'LONG')
        self.assertGreater(a['directional_confidence'], b['directional_confidence'])
        self.assertEqual(a['timeframes_used'], 4)

    def test_probability_calibration_requires_sample(self):
        hist = [{'status': 'win', 'r': 1, 'side': 'LONG', 'tf_probs': {'1d': 70, '4h': 70, '1h': 70, '15m': 70}} for _ in range(5)]
        out = ix.probability_calibration(hist, 70, 'LONG', min_sample=10)
        self.assertEqual(out['status'], 'warming_up')
        self.assertIsNone(out['calibrated_probability'])

    def test_probability_calibration_uses_closed_outcomes_only(self):
        hist = [{'status': 'win', 'r': 1, 'side': 'LONG', 'tf_probs': {'1d': 70, '4h': 70, '1h': 70, '15m': 70}} for _ in range(12)]
        hist += [{'status': 'active', 'r': 100, 'side': 'LONG', 'tf_probs': {'1d': 70, '4h': 70, '1h': 70, '15m': 70}}]
        out = ix.probability_calibration(hist, 70, 'LONG', min_sample=10)
        self.assertEqual(out['status'], 'calibrated')
        self.assertEqual(out['sample'], 12)
        self.assertLess(out['calibrated_probability'], 100)

    def test_entry_timing_blocks_far_entry(self):
        signal = {'price': 100, 'entry': 110, 'side': 'LONG'}
        tf = {'4h': {'atr': 1.0}}
        out = ix.entry_timing_assessment(signal, tf)
        self.assertTrue(out['blocked'])
        self.assertLessEqual(out['score'], 35)

    def test_reversal_risk_increases_with_opposing_evidence(self):
        tf = {k: {'long_prob': v} for k, v in {'15m': 20, '1h': 25, '4h': 35, '1d': 40}.items()}
        out = ix.reversal_probability_assessment('LONG', tf, {'state': 'direction_reversal_watch'}, {'conflict_pct': 80})
        self.assertGreaterEqual(out['risk_score'], 55)
        self.assertIn(out['level'], ('high', 'critical'))

    def test_adaptive_threshold_needs_minimum_sample(self):
        hist = [{'status': 'loss', 'r': -1} for _ in range(8)]
        self.assertEqual(ix.adaptive_confidence_threshold(hist, 66, min_sample=20)['adjustment'], 0)

    def test_portfolio_exposure_detects_same_side_crowding(self):
        hist = [{'coin': c, 'side': 'SHORT', 'status': 'in_trade'} for c in ('BTC', 'ETH', 'SOL')]
        out = ix.portfolio_exposure_assessment(hist)
        self.assertEqual(out['side_counts']['SHORT'], 3)
        self.assertEqual(out['risk_level'], 'high')

    def test_no_trade_gate_blocks_weak_confidence(self):
        out = ix.no_trade_decision({'directional_confidence': 54}, {'blocked': False},
                                   {'risk_score': 20}, {'threshold': 66})
        self.assertTrue(out['blocked'])
        self.assertEqual(out['action'], 'WAIT')
