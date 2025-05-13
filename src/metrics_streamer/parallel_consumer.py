from kafka import KafkaConsumer
import json
import signal
import logging
import sys
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from .config import KAFKA_CONFIG

# Import sketch algorithms from QuantileFlow
from QuantileFlow.ddsketch import DDSketch as QuantileFlowDDSketch
from ddsketch import DDSketch as DatadogDDSketch
from QuantileFlow.momentsketch import MomentSketch as QuantileFlowMomentSketch

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def deserialize_message(msg):
    return json.loads(msg.decode('utf-8'))

class LatencyMonitor:
    def __init__(self, window_size=100, refresh_interval=0.0001, dd_accuracy=0.01, moment_count=10):
        self.window_size = window_size
        self.running = True
        self.start_time = time.time()
        self.last_refresh = 0
        self.refresh_interval = refresh_interval
        self.msg_count = 0
        self.msg_count_lock = Lock()
        self.sketch_lock = Lock()
        
        # Initialize sketches for quantile computation
        """
        self.dd_sketch = DDSketch(dd_accuracy)  # DDSketch with 1% relative accuracy
        """
        #self.moment_sketch = MomentSketch(moment_count)  # MomentSketch with 10 moments
        self.datadog_dd_sketch = DatadogDDSketch(dd_accuracy)
        self.quantileflow_dd_sketch = QuantileFlowDDSketch(dd_accuracy)
        self.quantileflow_moment_sketch = QuantileFlowMomentSketch(moment_count)
        
        # Thread pool for parallel processing
        self.executor = ThreadPoolExecutor(max_workers=4)  # One worker per partition
        
    def _create_consumer(self):
        return KafkaConsumer(
            KAFKA_CONFIG['topic'],
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            group_id=KAFKA_CONFIG['group_id'],
            client_id=f"{KAFKA_CONFIG['client_id']}-consumer",
            value_deserializer=deserialize_message,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            max_poll_interval_ms=300000,
            max_poll_records=500,  # Increased batch size
            fetch_max_bytes=52428800,  # 50MB
            fetch_max_wait_ms=500,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000
        )

    def process_metrics(self):
        consumer = self._create_consumer()
        try:
            signal.signal(signal.SIGTERM, self.shutdown)
            signal.signal(signal.SIGINT, self.shutdown)
            
            while self.running:
                records = consumer.poll(timeout_ms=1000)  # Reduced timeout for more frequent polling
                if not records:
                    continue
                    
                # Process each partition in parallel
                futures = []
                for tp, messages in records.items():
                    if messages:
                        future = self.executor.submit(self._process_partition, messages)
                        futures.append(future)
                
                # Wait for all processing to complete
                for future in futures:
                    future.result()
                    
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            self.executor.shutdown(wait=True)
            consumer.close()

    def _process_partition(self, messages):
        """Process a batch of messages from a single partition"""
        for record in messages:
            self._process_record(record)

    def calculate_stats(self):
        with self.sketch_lock:
            stats = {
                'count': self.quantileflow_dd_sketch.count,
                'mean': 0
            }
            
            if stats['count'] > 0:
                # DDSketch metrics
                
                stats['dd_p50'] = self.quantileflow_dd_sketch.quantile(0.5)
                stats['dd_p95'] = self.quantileflow_dd_sketch.quantile(0.95)
                stats['dd_p99'] = self.quantileflow_dd_sketch.quantile(0.99)
                
                # MomentSketch metrics
                #stats['moment_p50'] = self.moment_sketch.quantile(0.5)
                #stats['moment_p95'] = self.moment_sketch.quantile(0.95)
                #stats['moment_p99'] = self.moment_sketch.quantile(0.99)
                """
                # Datadog DDSketch metrics
                stats['datadog_dd_p50'] = self.datadog_dd_sketch.get_quantile_value(0.5)
                stats['datadog_dd_p95'] = self.datadog_dd_sketch.get_quantile_value(0.95)
                stats['datadog_dd_p99'] = self.datadog_dd_sketch.get_quantile_value(0.99)
                """
                # Get summary statistics from moment_sketch
                #summary = self.moment_sketch.summary_statistics()
                #stats['mean'] = summary.get('mean', 0)
                
        return stats

    def _process_record(self, record):
        try:
            data = record.value
            latency = data['latency']
            
            with self.sketch_lock:
                self.quantileflow_dd_sketch.insert(latency)
            
            #self.quantileflow_moment_sketch.insert(latency)

            # self.datadog_dd_sketch.add(float(latency))
            
            with self.msg_count_lock:
                self.msg_count += 1
            
            stats = self.calculate_stats()
            self._print_metrics(data, stats)
        except Exception as e:
            logger.error(f"Error processing record: {e}")
    
    def _print_metrics(self, data, stats):
        current_time = time.time()
        
        if current_time - self.last_refresh < self.refresh_interval:
            return
        
        self.last_refresh = current_time
        elapsed = current_time - self.start_time
        throughput = self.msg_count / elapsed if elapsed > 0 else 0
        
        sys.stdout.write("\033[2J\033[H")
        sys.stdout.write(f"{'='*80}\n")
        sys.stdout.write(f"Metrics Streamer Latency Monitor Dashboard - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        sys.stdout.write(f"{'='*80}\n\n")
        
        sys.stdout.write(f"Block ID: {data['block_id']}\n")
        sys.stdout.write(f"Current Latency: {data['latency']:>8.2f} ms\n\n")
        
        sys.stdout.write("Performance Metrics:\n")
        sys.stdout.write(f"Messages Processed: {self.msg_count:>8.0f}\n")
        sys.stdout.write(f"Data Points in Sketch: {stats['count']:>8.0f}\n")
        sys.stdout.write(f"Runtime: {elapsed:>8.2f} sec\n")
        sys.stdout.write(f"Throughput: {throughput:>8.2f} msg/sec\n")
        sys.stdout.write(f"Refresh Rate: {self.refresh_interval:>8.2f} sec\n\n")
        
        sys.stdout.write("Latency Statistics:\n")
        sys.stdout.write(f"  Mean: {stats['mean']:>8.2f} ms\n\n")
        
        sys.stdout.write("QuantileFlow DDSketch Percentiles:\n")
        sys.stdout.write(f"  P50:  {stats.get('dd_p50', 0):>8.2f} ms\n")
        sys.stdout.write(f"  P95:  {stats.get('dd_p95', 0):>8.2f} ms\n")
        sys.stdout.write(f"  P99:  {stats.get('dd_p99', 0):>8.2f} ms\n\n")
        
        #sys.stdout.write("MomentSketch Percentiles:\n")
        #sys.stdout.write(f"  P50:  {stats.get('moment_p50', 0):>8.2f} ms\n")
        #sys.stdout.write(f"  P95:  {stats.get('moment_p95', 0):>8.2f} ms\n")
        #sys.stdout.write(f"  P99:  {stats.get('moment_p99', 0):>8.2f} ms\n")
        """
        sys.stdout.write("Datadog DDSketch Percentiles:\n")
        sys.stdout.write(f"  P50:  {stats.get('datadog_dd_p50', 0):>8.2f} ms\n")
        sys.stdout.write(f"  P95:  {stats.get('datadog_dd_p95', 0):>8.2f} ms\n")
        sys.stdout.write(f"  P99:  {stats.get('datadog_dd_p99', 0):>8.2f} ms\n")
        """
        sys.stdout.write(f"\n{'='*80}\n")
        sys.stdout.flush()

    def shutdown(self, signum, frame):
        logger.info("Shutting down consumer...")
        self.running = False
