from usage_processor import UsageProcessor
import pandas as pd
from pathlib import Path 


base_path = Path("D:/capstone1/data/landing")

files = [
base_path / f"sms-call-internet-mi-2013-11-{day:02d}.csv"
for day in range(1, 8)]

df = pd.concat(
[pd.read_csv(file) for file in files],
ignore_index=True)

processor = UsageProcessor(
    dataframe=df
)

# 1. Load
processor.load_data()

# 2. Clean
processor.clean_data()

# 3. Time features
processor.derive_time_features()

# 4. Grid/hour aggregation
processor.aggregate_to_grid_time()

# 5. Activity features
processor.derive_activity_features()

# 6. KPIs
kpis = processor.compute_kpis()

# 7. Export
paths = processor.export_summary(
    "curated_data_all"
)

print(kpis)
print(paths)