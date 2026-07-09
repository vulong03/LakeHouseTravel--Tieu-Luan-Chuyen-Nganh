import mlflow
import numpy as np

mlflow.set_tracking_uri("http://mlflow:5000")
runs = mlflow.search_runs(experiment_names=["province_hotel_volume_forecasting_lstm"], order_by=["start_time DESC"])

seed_runs = []
for idx, r in runs.iterrows():
    run_name = r.get("tags.mlflow.runName")
    if isinstance(run_name, str) and "seed_" in run_name:
        seed_runs.append(r)
    if len(seed_runs) == 3:
        break

keys = [
    "train_r2_actual", "train_wape_actual", "train_smape_actual",
    "val_r2_actual", "val_wape_actual", "val_smape_actual",
    "test_r2_actual", "test_wape_actual", "test_smape_actual"
]

print("=" * 60)
print("INDIVIDUAL SEED RESULTS")
print("=" * 60)
for r in seed_runs:
    run_name = r.get("tags.mlflow.runName")
    print(f"\n{run_name}:")
    for k in keys:
        print(f"  {k:<20}: {r.get('metrics.' + k)}")

print("\n" + "=" * 60)
print("AVERAGE METRICS ACROSS SEEDS (42, 100, 2026)")
print("=" * 60)
for k in keys:
    vals = [float(r.get("metrics." + k)) for r in seed_runs if r.get("metrics." + k) is not None]
    if vals:
        print(f"  {k:<20}: {np.mean(vals):.4f} +/- {np.std(vals):.4f}")
print("=" * 60)
