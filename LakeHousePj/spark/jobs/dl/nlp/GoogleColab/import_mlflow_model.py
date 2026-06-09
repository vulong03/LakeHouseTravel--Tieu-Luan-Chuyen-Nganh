import sys
sys.path.append('/opt/spark/jobs')
sys.path.append('/opt/spark/jobs/dl/nlp')


import torch
import mlflow
import mlflow.pytorch
from config import (
    MLFLOW_TRACKING_URI, MLFLOW_EXPERIMENT, 
    FINE_TUNED_MODEL_NAME, PHOBERT_MODEL_NAME, 
    SENTIMENT_LABELS, ASPECT_LABELS, INTENT_LABELS
)
# Re-use the model definition from train_phobert
from train_phobert import PhoBERTMultiTask

def main():
    print("=" * 70)
    print("NLP Utility: Register Colab Model to Local MLflow")
    print("=" * 70)

    device = torch.device("cpu")
    model = PhoBERTMultiTask(
        model_name=PHOBERT_MODEL_NAME,
        num_sentiments=len(SENTIMENT_LABELS),
        num_aspects=len(ASPECT_LABELS),
        num_intents=len(INTENT_LABELS),
    )

    # Path to downloaded model weights
    weights_path = "/data/GoogleColab/phobert_multi_task.pt"

    try:
        model.load_state_dict(torch.load(weights_path, map_location=device))
        print(f"✓ Loaded model weights successfully from local path: {weights_path}")
    except FileNotFoundError:
        print(f"❌ ERROR: File not found at '{weights_path}'")
        print("  Please download 'phobert_multi_task.pt' from Google Colab and place it in the local 'data/GoogleColab/' directory.")
        return
    except Exception as e:
        print(f"❌ ERROR loading weights: {e}")
        return

    # Log & Register to local MLflow Server
    print(f"\nConnecting to local MLflow at: {MLFLOW_TRACKING_URI}")
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    with mlflow.start_run(run_name="colab_phobert_import"):
        
        mlflow.log_param("model_name", PHOBERT_MODEL_NAME)
        mlflow.log_param("fine_tuned_model_name", FINE_TUNED_MODEL_NAME)
        mlflow.log_param("num_sentiment_labels", len(SENTIMENT_LABELS))
        mlflow.log_param("num_aspect_labels", len(ASPECT_LABELS))
        mlflow.log_param("num_intent_labels", len(INTENT_LABELS))
        mlflow.log_param("weights_path", weights_path)
        mlflow.log_param("import_source", "google_colab")
        mlflow.log_param("device", str(device))        
        # Log model
        mlflow.pytorch.log_model(model, "model")
        
        # Register model to registry
        model_uri = f"runs:/{mlflow.active_run().info.run_id}/model"
        mlflow.register_model(model_uri, FINE_TUNED_MODEL_NAME)
        
        print(f"\n🎉 SUCCESS: Model registered in local MLflow registry!")
        print(f"  Model Name : {FINE_TUNED_MODEL_NAME}")
        print(f"  Next step  : You can now run inference_phobert.py locally")

if __name__ == "__main__":
    main()
