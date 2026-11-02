import unittest
import pandas as pd
import ixcly_brain as brain


class BrainTests(unittest.TestCase):
    def setUp(self):
        closes = [100 + i * 0.4 for i in range(40)]
        self.df = pd.DataFrame({
            "close": closes,
            "high": [x + 1 for x in closes],
            "low": [x - 1 for x in closes],
            "volume": [1000] * 39 + [1800],
        })

    def test_market_fingerprint(self):
        x = brain.market_fingerprint("BTC", self.df, [])
        self.assertEqual(x["status"], "ok")
        self.assertEqual(x["trend"], "up")
        self.assertEqual(x["volume_ratio"], 1.8)

    def test_signal_dna_and_decision(self):
        dna = brain.signal_fingerprint({
            "strength": 86, "side": "LONG", "tf_consensus": "4/4",
            "bt": {"winrate": 70, "avg_r": .8}
        }, context={"orderbook_bid_share": .65, "news_risk": {"severity": "low"}})
        self.assertGreaterEqual(dna["score"], 70)
        decision = brain.decision_brain(dna, {}, {"level": "low"}, {"severity": "low"})
        self.assertIn(decision["action"], ("ENTER_CANDIDATE", "RECHECK"))
        self.assertFalse(decision["trade_execution"])

    def test_liquidity_path(self):
        x = brain.liquidity_path(self.df, .62)
        self.assertIn(x["status"], ("ok", "insufficient_data"))
        self.assertIn("levels", x)

    def test_news_divergence(self):
        df = pd.DataFrame({"close": [100, 100, 100, 98], "high": [101]*4,
                           "low": [97]*4, "volume": [100]*4})
        x = brain.news_reaction_assessment({"bias": 1, "severity": "high", "relevant": True}, df)
        self.assertTrue(x["divergence"])

    def test_strategy_and_counterfactual(self):
        hist = [{"status": "win", "r": 1.2, "coin": "BTC", "type": "trend",
                 "market_regime": "uptrend", "mfe": 1.8, "mae": -.2} for _ in range(6)]
        hist += [{"status": "loss", "r": -1, "coin": "BTC", "type": "trend",
                  "market_regime": "uptrend", "mfe": .9, "mae": -1.1} for _ in range(6)]
        self.assertTrue(brain.strategy_dna(hist))
        x = brain.counterfactual_lab(hist)
        self.assertEqual(x["trades"], 12)
        self.assertTrue(x["findings"])

    def test_replay_conservative_stop_first(self):
        sig = {"side": "LONG", "entry": 100, "sl1": 95,
               "tps": [105, 110, 115], "ts": 100}
        rows = [[100000, 100, 106, 94, 101, 1000]]
        x = brain.replay_signal(sig, rows)
        self.assertEqual(x["result"], "stop")
        self.assertTrue(x["entered"])


if __name__ == "__main__":
    unittest.main()
