from GPUQuantile.ddsketch import DDSketch
from GPUQuantile.momentsketch import MomentSketch
import numpy as np

# Generate random data
data = np.random.normal(0, 1, 1000)

# Initialize sketches
dd_sketch = DDSketch(0.01)  # 1% relative accuracy
moment_sketch = MomentSketch(10)  # 10 moments

# Insert data into sketches
for val in data:
    dd_sketch.insert(val)
    moment_sketch.insert(val)

# Compute percentiles
print('True P50:', np.percentile(data, 50))
print('True P95:', np.percentile(data, 95))
print('True P99:', np.percentile(data, 99))
print()
print('DDSketch P50:', dd_sketch.quantile(0.5))
print('DDSketch P95:', dd_sketch.quantile(0.95))
print('DDSketch P99:', dd_sketch.quantile(0.99))
print()
print('MomentSketch P50:', moment_sketch.quantile(0.5))
print('MomentSketch P95:', moment_sketch.quantile(0.95))
print('MomentSketch P99:', moment_sketch.quantile(0.99)) 