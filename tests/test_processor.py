import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from metrics_worker import TelemetryProcessor


class TestTelemetryProcessor(unittest.TestCase):
    def test_process_valid_batches(self):
        proc = TelemetryProcessor(max_workers=2)
        batches = {
            "sensor_a": [10.0, 12.0, 11.0, 13.0],
            "sensor_b": [20.0, 22.0, 21.0, 100.0, 20.5, 21.5, 22.0, 21.0],
        }
        res = proc.process_batches(batches)
        self.assertEqual(res["processed"], 2)
        self.assertEqual(res["failed"], 0)
        self.assertIn("sensor_a", res["batches"])
        self.assertEqual(res["batches"]["sensor_b"]["anomalies"], [(3, 100.0)])


if __name__ == "__main__":
    unittest.main()
