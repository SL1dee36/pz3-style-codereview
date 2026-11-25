import math
import threading
import time


class TelemetryProcessor:
    def __init__(self, max_workers=4):
        self.max_workers = max_workers
        self.results = {}
        self.errors = []
        # self._old_cache = []

    def _calc_ewma(self, values):
        # проходим по значениям и считаем экспоненциальное среднее
        ewma = values[0]
        for v in values[1:]:
            ewma = 0.3 * v + 0.7 * ewma
        return ewma

    def _detect_anomalies(self, values):
        if len(values) < 2:
            return []
        mean = sum(values) / len(values)
        var = sum((x - mean) ** 2 for x in values) / len(values)
        std = math.sqrt(var)
        if std == 0:
            return []
        anomalies = []
        for i, v in enumerate(values):
            if abs(v - mean) / std > 2.5:
                anomalies.append((i, v))
        return anomalies

    def _process_single(self, batch_id, values):
        # проверяем что список не пустой
        if not values:
            self.errors.append((batch_id, "empty"))
            return
        # считаем статистику
        total = 0
        mn = values[0]
        mx = values[0]
        for v in values:
            total += v
            if v < mn:
                mn = v
            if v > mx:
                mx = v
        avg = total / len(values)
        ewma = self._calc_ewma(values)
        anomalies = self._detect_anomalies(values)
        # имитация небольшой задержки I/O
        time.sleep(0.01)
        self.results[batch_id] = {
            "count": len(values),
            "min": mn,
            "max": mx,
            "avg": round(avg, 4),
            "ewma": round(ewma, 4),
            "anomalies": anomalies,
        }

    def process_batches(self, batches):
        self.results = {}
        self.errors = []
        if not batches:
            return {}

        # разбиваем на чанки по max_workers
        items = list(batches.items())
        threads = []
        for i in range(0, len(items), self.max_workers):
            chunk = items[i : i + self.max_workers]
            for batch_id, values in chunk:
                t = threading.Thread(target=self._process_single, args=(batch_id, values))
                threads.append(t)
                t.start()
            for t in threads:
                t.join()
            threads = []

        # формируем итоговый отчет
        summary = {}
        if self.results:
            all_avgs = []
            for k in self.results:
                all_avgs.append(self.results[k]["avg"])
            g_mn = all_avgs[0]
            g_mx = all_avgs[0]
            g_tot = 0
            for a in all_avgs:
                g_tot += a
                if a < g_mn:
                    g_mn = a
                if a > g_mx:
                    g_mx = a
            summary = {
                "processed": len(self.results),
                "failed": len(self.errors),
                "global_avg": round(g_tot / len(all_avgs), 4),
                "min_batch_avg": g_mn,
                "max_batch_avg": g_mx,
                "batches": self.results,
                "errors": self.errors,
            }
        else:
            summary = {
                "processed": 0,
                "failed": len(self.errors),
                "batches": {},
                "errors": self.errors,
            }
        return summary
