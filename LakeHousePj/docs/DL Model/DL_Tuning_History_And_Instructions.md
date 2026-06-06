# LSTM Hyperparameter Tuning History & Next Steps

This document captures the recent tuning runs performed on June 5, 2026, aimed at reducing overfitting in the LSTM model forecasting province hotel review volume.

---

## 1. Objectives & Metrics Target

*   **Goal**: Minimize the R2 score gap between training and testing datasets to **0.05 or lower** (i.e. `Train R2 - Test R2 <= 0.05`).
*   **Target Metric**: Keep `Test R2` as high as possible, preferably **>= 0.90**.
*   **Target Variable**: `hotel_review_volume` (Log transformed as `log1p(hotel_review_volume)`).

---

## 2. Tuning History & Experiment Runs

| Run Version | Key Hyperparameters / Config | Train R2 | Test R2 | R2 Gap | RMSE (Test) | Status / Decision |
| :--- | :--- | :---: | :---: | :---: | :---: | :--- |
| **v4 - baseline** | `hidden=48`, `dropout=0.4`, `wd=2e-4`, `batch=16` | 0.9850 | 0.9130 | **0.0720** | 0.4940 | Starting point after removing the noisy `avg_shares_per_post` feature. |
| **Round 1** | `hidden=48`, `dropout=0.4`, `wd=5e-4`, `batch=32` | 0.9712 | 0.9043 | **0.0669** | 0.5191 | **Current Best Baseline**. Increasing batch size smoothed the gradients and reduced the gap. |
| **Round 2** | `hidden=32`, `dropout=0.45`, `wd=1e-3`, `batch=32` | 0.9555 | 0.8640 | **0.0915** | 0.6190 | **Rejected**. Reducing hidden units too much caused the model to lose key temporal representations. |
| **Round 3** | `hidden=40`, `dropout=0.42`, `wd=6e-4`, `batch=32` | 0.9691 | 0.8932 | **0.0760** | 0.5486 | **Rejected**. Reducing capacity too much caused Test R2 to fall below 0.90 without sufficiently narrowing the gap. |
| **Round 4** | `hidden=48`, `dropout=0.43`, `wd=7e-4`, `batch=32` | 0.9758 | 0.8992 | **0.0766** | 0.5328 | **Rejected**. Overfitting gap remained high (0.0766) and Test R2 was slightly below target. |
| **Round 5** | `hidden=48`, `dropout=0.43`, `wd=7e-4`, `batch=32` + 3 Custom Features | 0.9691 | 0.9245 | **0.0446** | 0.4612 | **Success**. Met both targets: `Test R2 >= 0.90` (achieved 0.9245) and `Gap <= 0.05` (achieved 0.0446). |
| **Round 6** | `hidden=48`, `dropout=0.43`, `wd=7e-4`, `batch=32` + 3 Features + **No BatchNorm** | 0.9754 | 0.9281 | **0.0473** | 0.4501 | **Success (Current Production)**. Removing BatchNorm1d resolved the batch-size-1 inference mismatch. Test MAPE dropped to **40.84%** (a 9.37% relative error reduction) and Test R2 improved. |

---

## 3. Next Steps & Current Status

The model performance targets have been **successfully met** through Feature Engineering (Option A) combined with BatchNorm removal. The model is ready for staging/production deployment.

### Current Hyperparameter Configuration
The current active configurations in [train_lstm_forecast.py](file:///d:/CodeStored/Nam_4/TieuLuanCuoiKy/LakeHouse/LakeHousePj/spark/jobs/ml/train_lstm_forecast.py) are:
*   `SEQUENCE_LENGTH = 3`
*   `HIDDEN_SIZE = 48`
*   `NUM_LAYERS = 2`
*   `DROPOUT = 0.43`
*   `LEARNING_RATE = 0.0005`
*   `BATCH_SIZE = 32`
*   `weight_decay = 7e-4`
*   `BatchNorm1d`: **Removed** (helps resolve recursive forecast batch size 1 mismatch)
*   New Features introduced: `social_to_booking_ratio`, `sentiment_polarity_change`, `hotel_vol_std_rolling_3m`

No further hyperparameter tuning is required for this pipeline stage. Let's proceed to other components of the LakeHouse project.
