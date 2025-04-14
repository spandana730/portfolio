# BARTNet: Context-Aware Deep Learning Framework for Ridership Forecasting

BARTNet is a deep learning-based framework designed to accurately forecast Bay Area Rapid Transit (BART) ridership by integrating temporal and contextual features such as weather, holidays, and station location. This project was accepted at IEEE CAI 2025.

## 🚆 Problem Statement
Traditional models like ARIMA fail to capture complex patterns in public transit ridership influenced by non-linear and external factors. BARTNet addresses this using advanced deep learning models.

## 🧠 Key Models Used
- LSTM with Attention
- Bidirectional LSTM with Attention
- Temporal Convolutional Network (TCN)
- Transformer
- Fine-tuned BERT
- Baseline: ARIMA

## 🛠️ Methodology
- Collected hourly BART ridership data (Apr 2020–Nov 2024)
- Integrated external context: weather (OpenMeteo), holidays, station metadata
- Applied sliding window (3 months) for time-series sequence generation
- Normalized and encoded features (lagged, categorical, one-hot, etc.)
- Evaluated models using RMSE, MAE, and MAPE

## 📊 Results (Top Performers)
| Model                         | RMSE  | MAE   | MAPE (%) |
|------------------------------|-------|-------|-----------|
| Bi-LSTM + Attention (Best)   | 2.100 | 1.908 | 3.248     |
| LSTM + Attention             | 2.663 | 1.998 | 3.516     |
| TCN                          | 3.627 | 3.070 | 5.164     |
| ARIMA (Baseline)             | 6.145 | 4.753 | 7.666     |

## 📁 Folders
- `notebooks/`: Contains preprocessing and modeling notebooks (to be added)
  
## 📄 Publication
This work has been accepted at IEEE CAI 2025. [Link to be added upon publication]
