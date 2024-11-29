import time
import threading
from collections import defaultdict
import queue
import sqlite3
import atexit



class MetricsCollector:
    def __init__(self):
        self.metrics = defaultdict(lambda: {"calls": 0, "total_time": 0.0, "errors": 0})
        self.queue = queue.Queue()
        self._setup_db()
        self._start_worker()
        self._init_metric()

    def get_metrics(self, function_name):
        """Retrieve metrics for a specific function."""
        if function_name not in self.metrics:
            return f"No metrics available for function: {function_name}"
        metrics = self.metrics[function_name]
        avg_time = metrics["total_time"] / metrics["calls"] if metrics["calls"] > 0 else 0
        return {
            "Function": function_name,
            "Number of calls": metrics["calls"],
            "Average execution time": avg_time,
            "Number of errors": metrics["errors"],
        }

    def _setup_db(self):
        """Set up SQLite database."""
        self.conn = sqlite3.connect("metrics.db", check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS metrics (
                function_name TEXT PRIMARY KEY,
                calls INTEGER,
                total_time REAL,
                errors INTEGER
            )
        """)
        self.conn.commit()

    def _start_worker(self):
        """Start a background thread to handle database writes."""
        self.worker_thread = threading.Thread(target=self._process_queue, daemon=True)
        self.worker_thread.start()
        atexit.register(self._cleanup)

    def _process_queue(self):
        """Process metrics from the queue and save them to the database."""
        while True:
            try:
                function_name, data = self.queue.get(timeout=1)
                self._save_to_db(function_name, data)
                self.queue.task_done()
            except queue.Empty:
                continue

    def _save_to_db(self, function_name, data):
        """Save metrics to the database."""
        self.cursor.execute("""
            INSERT INTO metrics (function_name, calls, total_time, errors)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(function_name)
            DO UPDATE SET
                    calls = excluded.calls,
                    total_time = excluded.total_time,
                    errors = excluded.errors
                WHERE function_name = ?    
        """, (function_name, data["calls"], data["total_time"], data["errors"], function_name))
        self.conn.commit()

    def _init_metric(self):
        metrics = self.cursor.execute('SELECT * FROM metrics').fetchall()
        for metric in metrics:
            self.metrics[metric[0]] = {
                "calls": metric[1],
                "total_time": metric[2],
                "errors": metric[3]
            }


    def _cleanup(self):
        """Ensure all data is saved before exiting."""
        self.queue.join()
        self.conn.close()

metrics_collector = MetricsCollector()

def collect_metrics(func):
    """Decorator for collecting function metrics."""

    def wrapper(*args, **kwargs):
        start_time = time.time()
        metrics_collector.metrics[func.__name__]["calls"] += 1
        result = None
        try:
            result = func(*args, **kwargs)
        except Exception:
            metrics_collector.metrics[func.__name__]["errors"] += 1
        finally:
            elapsed_time = time.time() - start_time
            metrics_collector.metrics[func.__name__]["total_time"] += elapsed_time

            # Send metrics to the queue for async processing
            metrics_collector.queue.put((func.__name__, {
                "calls": metrics_collector.metrics[func.__name__]["calls"],
                "total_time": metrics_collector.metrics[func.__name__]["total_time"],
                "errors": metrics_collector.metrics[func.__name__]["errors"],
            }))
        return result

    return wrapper
