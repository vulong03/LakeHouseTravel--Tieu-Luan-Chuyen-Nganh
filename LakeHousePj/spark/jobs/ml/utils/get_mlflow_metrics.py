import requests, json

base = "http://mlflow:5000/api/2.0/mlflow"
exp = requests.get(
    f"{base}/experiments/get-by-name",
    params={"experiment_name": "province_hotel_volume_forecasting_lstm"}
).json()
exp_id = exp["experiment"]["experiment_id"]

runs = requests.post(
    f"{base}/runs/search",
    json={"experiment_ids": [exp_id], "max_results": 1, "order_by": ["start_time DESC"]}
).json()

run = runs["runs"][0]
print("Run ID:", run["info"]["run_id"])
print("Status:", run["info"]["status"])
print("Metrics:")
for m in sorted(run.get("data", {}).get("metrics", []), key=lambda x: x["key"]):
    print(f"  {m['key']}: {m['value']:.6f}")
