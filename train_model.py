import pandas as pd
from sklearn.ensemble import IsolationForest
from joblib import dump

#Load historical data
data = pd.read_csv('historical_metrics.csv')

#Train Isolation Forest
model = IsolationForest(contamination=0.01)
model.fit(data[['reaction_time_ms']])

#Save model
dump(model, 'anomaly_detector.joblib')
print("Model saved!")