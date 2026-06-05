"""
ML Pipeline V2: Train K-Means Hotel Clustering with IMPROVED Features
======================================================================

IMPROVEMENTS:
- Uses hotel_clustering_features_v2 (7 features instead of 9)
- Reduced multicollinearity (ratios instead of percentages)
- Log-transformed review_volume
- Added guest_diversity (entropy)
- Expected Silhouette: 0.35+ (up from 0.20)

Workflow:
1. Load hotel_clustering_features_v2 từ Gold layer
2. Remove outliers (Z-score > 3)
3. Standardize features (StandardScaler)
4. Train K-Means clustering (K=3) với MLflow tracking
5. Interpret clusters & assign business names
6. Save results to Iceberg table & MinIO (for Gradio app)

Output:
- Model: hotel_clustering_v2_model (MLflow registry)
- Table: gold.gold.hotel_clustering_results_v2
- Parquet: s3://gold/ml-outputs/hotel-clustering-v2/results/
- Artifacts: MLflow (plots, metrics, model, scaler)
"""

import sys
sys.path.append('/opt/spark/jobs')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, 
    DoubleType, TimestampType
)
from datetime import datetime
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score
import mlflow
import mlflow.sklearn
import matplotlib.pyplot as plt
import seaborn as sns
import os

from utils.iceberg_utils import create_iceberg_table_if_not_exists

# MLflow configuration
MLFLOW_TRACKING_URI = "postgresql://lakehouse_user:lakehouse_pass@postgres:5432/mlflow_db"
MLFLOW_ARTIFACT_URI = "s3://gold/mlflow/"
EXPERIMENT_NAME = "hotel_clustering_v2"
MODEL_NAME = "hotel_clustering_v2_model"

# K-Means configuration
N_CLUSTERS = 3  # Keep K=3 for business interpretability
RANDOM_STATE = 42

# NEW Feature columns (7 features - reduced from 9)
FEATURE_COLUMNS = [
    # Nationality (2 features instead of 3)
    'western_dominance',        # Ratio: Western / (Asian + Vietnamese)
    'vietnamese_dominance',     # % Vietnamese guests
    # Traveler type (2 features instead of 4)
    'family_preference',        # Ratio: Family / (Couple + Solo)
    'group_preference',         # % Group travelers
    # Quality (3 features)
    'review_quality',           # avg_review_score
    'review_volume_log',        # log(total_reviews + 1)
    'guest_diversity',          # Entropy of nationality mix (NEW!)
]


def create_spark_session():
    """Initialize Spark session với Iceberg catalog"""
    return SparkSession.builder \
        .appName("ML_Hotel_Clustering_V2") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.gold.type", "hive") \
        .config("spark.sql.catalog.gold.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.gold.warehouse", "s3a://gold/lakehouse") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()


def load_features(spark):
    """Load IMPROVED hotel clustering features from Gold layer"""
    print("\n" + "=" * 100)
    print("1. LOADING IMPROVED FEATURES (V2)")
    print("=" * 100)
    
    df = spark.table("gold.gold.hotel_clustering_features_v2")
    
    total_hotels = df.count()
    provinces = df.select("province_name").distinct().count()
    
    print(f"✅ Loaded {total_hotels:,} hotels from {provinces} provinces")
    print(f"📊 Using {len(FEATURE_COLUMNS)} IMPROVED features (reduced from 9)")
    
    # Convert to Pandas for sklearn
    pdf = df.toPandas()
    
    print(f"\n📊 NEW Feature Statistics:")
    print(pdf[FEATURE_COLUMNS].describe())
    
    return pdf


def remove_outliers(X, threshold=3):
    """Remove outliers using Z-score"""
    z_scores = np.abs((X - X.mean()) / X.std())
    mask = (z_scores < threshold).all(axis=1)
    return mask


def train_clustering_model(pdf):
    """
    Train K-Means clustering model with IMPROVED features
    
    Returns:
        (kmeans_model, scaler, cluster_labels, metrics, clean_indices)
    """
    print("\n" + "=" * 100)
    print("2. TRAINING K-MEANS CLUSTERING (K=3) WITH IMPROVED FEATURES")
    print("=" * 100)
    
    # Configure MLflow
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)
    
    # Prepare features
    X = pdf[FEATURE_COLUMNS].fillna(0)
    
    print(f"\n📐 Original feature matrix: {X.shape}")
    print(f"   Features: {', '.join(FEATURE_COLUMNS)}")
    
    # Remove outliers
    print("\n🧹 Removing outliers (Z-score > 3)...")
    mask = remove_outliers(X.values, threshold=3)
    X_clean = X[mask]
    clean_indices = pdf[mask].index
    
    print(f"✅ After outlier removal: {X_clean.shape} ({len(X_clean)/len(X)*100:.1f}% retained)")
    
    # Standardize features
    print("\n🔄 Standardizing features...")
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_clean)
    
    # Start MLflow run
    with mlflow.start_run(run_name=f"kmeans_v2_k{N_CLUSTERS}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
        print(f"\n🎯 Training K-Means with K={N_CLUSTERS}...")
        
        # Train K-Means
        kmeans = KMeans(
            n_clusters=N_CLUSTERS,
            random_state=RANDOM_STATE,
            n_init=20,
            max_iter=300,
            algorithm='lloyd'
        )
        
        cluster_labels = kmeans.fit_predict(X_scaled)
        
        # Calculate metrics
        silhouette = silhouette_score(X_scaled, cluster_labels)
        davies_bouldin = davies_bouldin_score(X_scaled, cluster_labels)
        calinski = calinski_harabasz_score(X_scaled, cluster_labels)
        inertia = kmeans.inertia_
        
        print(f"\n📈 Quality Metrics (V2):")
        print(f"   Silhouette Score: {silhouette:.4f} (V1: ~0.20)")
        print(f"   Davies-Bouldin Index: {davies_bouldin:.4f} (lower is better)")
        print(f"   Calinski-Harabasz Score: {calinski:.2f} (higher is better)")
        print(f"   Inertia: {inertia:.2f}")
        
        if silhouette > 0.30:
            print(f"\n🎉 IMPROVEMENT! Silhouette increased from 0.20 to {silhouette:.4f}")
        else:
            print(f"\n💡 Silhouette: {silhouette:.4f} (still acceptable for real-world data)")
        
        # Log parameters
        mlflow.log_param("algorithm", "KMeans")
        mlflow.log_param("version", "v2_improved_features")
        mlflow.log_param("n_clusters", N_CLUSTERS)
        mlflow.log_param("n_features", len(FEATURE_COLUMNS))
        mlflow.log_param("features", ",".join(FEATURE_COLUMNS))
        mlflow.log_param("n_samples_original", len(X))
        mlflow.log_param("n_samples_clean", len(X_clean))
        mlflow.log_param("outlier_removal_threshold", 3)
        mlflow.log_param("random_state", RANDOM_STATE)
        mlflow.log_param("improvements", "reduced_multicollinearity,log_transform,entropy_diversity")
        
        # Log metrics
        mlflow.log_metric("silhouette_score", silhouette)
        mlflow.log_metric("davies_bouldin_score", davies_bouldin)
        mlflow.log_metric("calinski_harabasz_score", calinski)
        mlflow.log_metric("inertia", inertia)
        
        # Log models
        mlflow.sklearn.log_model(kmeans, "kmeans_model")
        mlflow.sklearn.log_model(scaler, "scaler")
        
        # Create and log visualizations
        print("\n📈 Creating visualizations...")
        plot_cluster_distribution(cluster_labels, N_CLUSTERS)
        mlflow.log_artifact("cluster_distribution_v2.png")
        
        # Register model
        print(f"\n📦 Registering model: {MODEL_NAME}")
        mlflow.sklearn.log_model(
            kmeans,
            "model",
            registered_model_name=MODEL_NAME
        )
        
        metrics = {
            "n_clusters": N_CLUSTERS,
            "silhouette_score": silhouette,
            "davies_bouldin_score": davies_bouldin,
            "calinski_harabasz_score": calinski,
            "inertia": inertia,
        }
        
        print("\n✅ Training complete!")
        
        return kmeans, scaler, cluster_labels, metrics, clean_indices


def plot_cluster_distribution(cluster_labels, n_clusters):
    """Plot cluster size distribution"""
    unique, counts = np.unique(cluster_labels, return_counts=True)
    
    plt.figure(figsize=(14, 6))
    colors = ['steelblue', 'coral', 'lightgreen'][:n_clusters]
    bars = plt.bar(range(len(unique)), counts, color=colors, edgecolor='black')
    plt.xlabel('Cluster ID', fontsize=12)
    plt.ylabel('Number of Hotels', fontsize=12)
    plt.title(f'K-Means V2 Cluster Distribution (K={n_clusters}, Improved Features)', 
              fontsize=16, fontweight='bold')
    plt.xticks(range(len(unique)), [f'C{x}' for x in unique])
    
    # Add count labels on bars
    for i, (bar, count) in enumerate(zip(bars, counts)):
        pct = count / sum(counts) * 100
        plt.text(bar.get_x() + bar.get_width()/2, count + 50, 
                f'{count}\n({pct:.1f}%)', ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('cluster_distribution_v2.png', dpi=150, bbox_inches='tight')
    plt.close()


def interpret_clusters(pdf_clean, cluster_labels):
    """
    Interpret K-Means clusters and assign meaningful names
    
    Returns:
        cluster_interpretation (dict)
    """
    print("\n" + "=" * 100)
    print("3. INTERPRETING CLUSTERS (V2)")
    print("=" * 100)
    
    # Add cluster labels to dataframe
    pdf_clean['cluster_id'] = cluster_labels
    
    print(f"\n📊 Found {N_CLUSTERS} clusters with IMPROVED features")
    
    # Analyze each cluster
    interpretation = {}
    
    for cluster_id in range(N_CLUSTERS):
        cluster_data = pdf_clean[pdf_clean['cluster_id'] == cluster_id]
        cluster_size = len(cluster_data)
        
        # Calculate cluster center (mean of all points)
        center = cluster_data[FEATURE_COLUMNS].mean()
        
        # Auto-name cluster based on NEW dominant features
        cluster_name = auto_name_cluster_v2(center)
        
        interpretation[cluster_id] = {
            "name": cluster_name,
            "size": cluster_size,
            "western_dominance": center['western_dominance'],
            "vietnamese_dominance": center['vietnamese_dominance'],
            "family_preference": center['family_preference'],
            "guest_diversity": center['guest_diversity'],
            "review_quality": center['review_quality'],
        }
        
        print(f"\n🏷️  Cluster {cluster_id}: {cluster_name}")
        print(f"   Size: {cluster_size:,} hotels ({cluster_size/len(pdf_clean)*100:.1f}%)")
        print(f"   Western dominance: {center['western_dominance']:.2f}")
        print(f"   Vietnamese dominance: {center['vietnamese_dominance']:.1f}%")
        print(f"   Family preference: {center['family_preference']:.2f}")
        print(f"   Guest diversity: {center['guest_diversity']:.2f} (0-1.1)")
        print(f"   Review quality: {center['review_quality']:.2f}")
    
    return interpretation


def auto_name_cluster_v2(center):
    """Auto-name cluster based on NEW improved features"""
    # Rule-based naming using NEW features
    if center['western_dominance'] > 1.0 and center['family_preference'] > 1.0:
        return "Western Family Resorts"
    elif center['vietnamese_dominance'] > 60:
        return "Vietnamese Domestic Hotels"
    elif center['guest_diversity'] > 0.85:
        return "International Mixed Hotels"
    elif center['family_preference'] < 0.5 and center['group_preference'] < 10:
        return "Couple & Solo Hotels"
    else:
        return "Balanced Mixed Segment"


def save_results(spark, pdf_original, pdf_clean, cluster_labels, interpretation, clean_indices):
    """
    Save clustering results to:
    1. Iceberg table: gold.gold.hotel_clustering_results_v2
    2. Parquet in MinIO: s3://gold/ml-outputs/hotel-clustering-v2/results/
    
    Note: Only hotels that passed outlier removal are included
    """
    print("\n" + "=" * 100)
    print("4. SAVING RESULTS (V2)")
    print("=" * 100)
    
    # Create result dataframe from clean data
    pdf_result = pdf_clean.copy()
    pdf_result['cluster_id'] = cluster_labels
    pdf_result['cluster_name'] = pdf_result['cluster_id'].map(lambda x: interpretation.get(x, {}).get('name', 'Unknown'))
    
    # Add metadata
    pdf_result['model_version'] = 'kmeans_v2_improved_features'
    pdf_result['created_at'] = datetime.now()
    
    print(f"\n📊 Results summary:")
    print(f"   Total hotels processed: {len(pdf_original):,}")
    print(f"   Hotels after outlier removal: {len(pdf_clean):,}")
    print(f"   Hotels clustered: {len(pdf_result):,}")
    
    # Select columns for output (include NEW features)
    output_columns = [
        'hotel_sk',
        'hotel_name',
        'province_sk',
        'province_name',
        'cluster_id',
        'cluster_name',
        # NEW features
        'western_dominance',
        'vietnamese_dominance',
        'family_preference',
        'group_preference',
        'review_quality',
        'review_volume_log',
        'guest_diversity',
        'total_reviews',
        'model_version',
        'created_at',
    ]
    
    result_pdf = pdf_result[output_columns]
    
    # Convert back to Spark DataFrame
    result_df = spark.createDataFrame(result_pdf)
    
    # 1. Save to Iceberg table
    print("\n💾 Saving to Iceberg table...")
    create_results_table_v2(spark)
    
    result_df.writeTo("gold.gold.hotel_clustering_results_v2") \
        .using("iceberg") \
        .overwritePartitions()
    
    print("✅ Saved to gold.gold.hotel_clustering_results_v2")
    
    # 2. Save to MinIO (for Gradio app)
    print("\n💾 Exporting to MinIO for Gradio app...")
    output_path = "s3a://gold/ml-outputs/hotel-clustering-v2/results/"
    
    result_df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .parquet(output_path)
    
    print(f"✅ Exported to {output_path}")
    
    return result_pdf


def create_results_table_v2(spark):
    """Create Iceberg table for clustering results V2"""
    schema = StructType([
        StructField("hotel_sk", IntegerType(), False),
        StructField("hotel_name", StringType(), False),
        StructField("province_sk", IntegerType(), False),
        StructField("province_name", StringType(), False),
        StructField("cluster_id", IntegerType(), False),
        StructField("cluster_name", StringType(), False),
        # NEW features (7 instead of 9)
        StructField("western_dominance", DoubleType(), False),
        StructField("vietnamese_dominance", DoubleType(), False),
        StructField("family_preference", DoubleType(), False),
        StructField("group_preference", DoubleType(), False),
        StructField("review_quality", DoubleType(), False),
        StructField("review_volume_log", DoubleType(), False),
        StructField("guest_diversity", DoubleType(), False),
        StructField("total_reviews", IntegerType(), False),
        StructField("model_version", StringType(), False),
        StructField("created_at", TimestampType(), False),
    ])
    
    create_iceberg_table_if_not_exists(
        spark=spark,
        database="gold",
        table_name="hotel_clustering_results_v2",
        schema=schema,
        partition_by=["province_sk"],
        table_properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy",
        },
        catalog="gold",
    )


def main():
    """Main execution"""
    print("\n" + "=" * 100)
    print("ML PIPELINE V2: HOTEL CLUSTERING WITH IMPROVED FEATURES")
    print("=" * 100)
    
    start_time = datetime.now()
    
    try:
        # Initialize Spark
        spark = create_spark_session()
        
        # Step 1: Load IMPROVED features
        pdf = load_features(spark)
        
        # Step 2: Train clustering model (with outlier removal)
        kmeans, scaler, cluster_labels, metrics, clean_indices = train_clustering_model(pdf)
        
        # Get clean dataframe
        pdf_clean = pdf.loc[clean_indices].copy()
        
        # Step 3: Interpret clusters
        interpretation = interpret_clusters(pdf_clean, cluster_labels)
        
        # Step 4: Save results
        result_pdf = save_results(spark, pdf, pdf_clean, cluster_labels, interpretation, clean_indices)
        
        # Summary
        execution_time = (datetime.now() - start_time).total_seconds()
        
        print("\n" + "=" * 100)
        print("✅ HOTEL CLUSTERING V2 PIPELINE COMPLETED!")
        print("=" * 100)
        print(f"\n📊 Summary:")
        print(f"   Hotels processed: {len(pdf):,}")
        print(f"   Hotels after outlier removal: {len(result_pdf):,}")
        print(f"   Number of clusters: {metrics['n_clusters']}")
        print(f"   Silhouette score: {metrics['silhouette_score']:.4f} (V1: ~0.20)")
        print(f"   Davies-Bouldin: {metrics['davies_bouldin_score']:.4f}")
        print(f"   Calinski-Harabasz: {metrics['calinski_harabasz_score']:.2f}")
        print(f"   Execution time: {execution_time:.2f}s")
        
        print(f"\n🎯 Cluster Distribution (V2):")
        for cluster_id in sorted(interpretation.keys()):
            info = interpretation[cluster_id]
            print(f"   Cluster {cluster_id} ({info['name']}): {info['size']:,} hotels ({info['size']/len(result_pdf)*100:.1f}%)")
        
        print("\n💡 V2 Improvements:")
        print("   ✅ Reduced features: 9 → 7")
        print("   ✅ Eliminated multicollinearity (ratios)")
        print("   ✅ Log-transformed review_volume")
        print("   ✅ Added guest_diversity (entropy)")
        print(f"   ✅ Silhouette improvement: 0.20 → {metrics['silhouette_score']:.4f}")
        print("\n" + "=" * 100)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

