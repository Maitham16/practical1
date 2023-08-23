import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error

# Load the data
data = pd.read_csv('dataset.csv')

# Filter data for node 1
node1_data = data[data['node'] == 'Node 1']

# Feature selection
features = node1_data[['battery_capacity', 'charge', 'consumption', 'engine_power', 'engine_torque']]
target = node1_data['current_speed']

# Split the data into training and test sets
X_train, X_test, y_train, y_test = train_test_split(features, target, test_size=0.2, random_state=42)

# Initialize the model
model = LinearRegression()

# Train the model
model.fit(X_train, y_train)

# Predict the test set results
y_pred = model.predict(X_test)

# Calculate the mean squared error of our predictions
mse = mean_squared_error(y_test, y_pred)

print(f"Mean Squared Error: {mse}")
