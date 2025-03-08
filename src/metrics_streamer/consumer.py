from kafka import KafkaConsumer
import json
import signal
import logging
from collections import deque
import numpy as np
import sys
import time
from datetime import datetime
from .config import KAFKA_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def deserialize_message(msg):
    return json.loads(msg.decode('utf-8'))

class LatencyMonitor:
    def __init__(self, window_size=1000, refresh_interval=0.0001):
        self.window_size = window_size
        self.values = deque(maxlen=window_size)
        self.running = True
        self.start_time = time.time()
        self.last_refresh = 0
        self.refresh_interval = refresh_interval
        self.msg_count = 0
        
    def _create_consumer(self):
        return KafkaConsumer(
            KAFKA_CONFIG['topic'],
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            group_id=KAFKA_CONFIG['group_id'],
            client_id=f"{KAFKA_CONFIG['client_id']}-consumer",
            value_deserializer=deserialize_message,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            max_poll_interval_ms=300000
        )

    def process_metrics(self):
        consumer = self._create_consumer()
        try:
            signal.signal(signal.SIGTERM, self.shutdown)
            signal.signal(signal.SIGINT, self.shutdown)
            
            while self.running:
                records = consumer.poll(timeout_ms=3000)
                for tp, messages in records.items():
                    for record in messages:
                        self._process_record(record)
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            consumer.close()

    def calculate_stats(self):
        if not self.values:
            return {}
        return {
            'count': len(self.values),
            'mean': np.mean(self.values),
            'p50': np.percentile(self.values, 50),
            'p95': np.percentile(self.values, 95),
            'p99': np.percentile(self.values, 99)
        }

    def _process_record(self, record):
        try:
            data = record.value
            self.values.append(data['latency'])
            stats = self.calculate_stats()
            # self._print_stats(data['block_id'], data['latency'], stats)
            self._print_metrics(data, stats)
        except Exception as e:
            logger.error(f"Error processing record: {e}")

    def _print_stats(self, block_id, latency, stats):
        print(f"\rBlockId: {block_id} | "
            f"Current: {latency:>8.2f} ms | "
            f"Mean: {stats['mean']:>8.2f} ms | "
            f"P50: {stats['p50']:>8.2f} ms | "
            f"P95: {stats['p95']:>8.2f} ms | "
            f"P99: {stats['p99']:>8.2f} ms", end="")
    
    def _print_metrics(self, data, stats):
        current_time = time.time()
        
        # Only refresh display if interval has elapsed
        if current_time - self.last_refresh < self.refresh_interval:
            return
        
        self.last_refresh = current_time
        elapsed = current_time - self.start_time
        self.msg_count += 1
        throughput = self.msg_count / elapsed if elapsed > 0 else 0
        
        sys.stdout.write("\033[2J\033[H")  # Clear screen
        sys.stdout.write(f"{'='*80}\n")
        sys.stdout.write(f"Latency Monitor Dashboard - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        sys.stdout.write(f"{'='*80}\n\n")
        
        # Block Information
        sys.stdout.write(f"Block ID: {data['block_id']}\n")
        sys.stdout.write(f"Current Latency: {data['latency']:>8.2f} ms\n\n")
        
        # Performance Metrics
        sys.stdout.write("Performance Metrics:\n")
        sys.stdout.write(f"Messages Processed: {self.msg_count:>8d}\n")
        sys.stdout.write(f"Runtime: {elapsed:>8.2f} sec\n")
        sys.stdout.write(f"Throughput: {throughput:>8.2f} msg/sec\n")
        sys.stdout.write(f"Refresh Rate: {self.refresh_interval:>8.2f} sec\n")
        sys.stdout.write(f"Window Size: {len(self.values):>8d}\n\n")
        
        # Statistics
        sys.stdout.write("Latency Statistics:\n")
        sys.stdout.write(f"  Mean: {stats['mean']:>8.2f} ms\n")
        sys.stdout.write(f"  P50:  {stats['p50']:>8.2f} ms\n")
        sys.stdout.write(f"  P95:  {stats['p95']:>8.2f} ms\n")
        sys.stdout.write(f"  P99:  {stats['p99']:>8.2f} ms\n")
        sys.stdout.write(f"\n{'='*80}\n")
        sys.stdout.flush()

    def shutdown(self, signum, frame):
        logger.info("Shutting down consumer...")
        self.running = False
