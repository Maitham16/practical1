# %%
# imports
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.discriminant_analysis import StandardScaler


# %%
# Load the data
data = pd.read_csv('node2_data.csv')

# %%
data['needs_charge'] = np.where(data['charge'] <= 40, 1, 0)

# %% [markdown]
# DATA CLEANING

# %%
# Data Cleaning
data = pd.get_dummies(data, columns=['weather', 'traffic', 'road_gradient', 'emergency', 'car_status'])

# Check the column names
print(data.columns)

# %%
## Remove duplicates
data = data.drop_duplicates()

# Drop other non-numeric columns if any
data = data.select_dtypes(include=[float, int])

# %%
## Handle outliers
Q1 = data.quantile(0.25)
Q3 = data.quantile(0.75)
IQR = Q3 - Q1
data = data[~((data < (Q1 - 1.5 * IQR)) | (data > (Q3 + 1.5 * IQR))).any(axis=1)]

# %% [markdown]
# Feature Selection

# %%
# Feature Selection
features = ['current_speed', 'battery_capacity', 'charge', 'consumption', 'distance_covered', 'battery_life', 'distance_to_charging_point']
features += [col for col in data if 'weather_' in col]
features += [col for col in data if 'traffic_' in col]
features += [col for col in data if 'road_gradient_' in col]
features += [col for col in data if 'emergency_' in col]
features += [col for col in data if 'car_status_' in col]

X = data[features]
y = data['needs_charge']

print(X.head())

# %%
print(data['charge'].describe())
print(data['needs_charge'].value_counts())


# %%
print(data['needs_charge'].value_counts())

# %%
# Data Splitting
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

# %%
# Scale the features
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# %% [markdown]
# Data Visualization

# %%
# Distribution of the target variable
plt.figure(figsize=(12,8))
sns.histplot(data['needs_charge'], kde=True)
plt.title('Distribution of Needs Charging Rate')
plt.xlabel('Needs Charging Rate')
plt.ylabel('Count')
plt.show()

# %%
# plot the distribution 
plt.figure(figsize=(12,8))
sns.histplot(data['charge'], kde=True)
plt.title('Distribution of Charge')
plt.xlabel('Charge')
plt.ylabel('Count')
plt.show()


# %%
## Correlation Analysis
correlation_matrix = data.corr()
plt.figure(figsize=(30,15))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm')
plt.title('Correlation Matrix')
plt.show()

# %%
# Visualizing the distribution of the features
data[features].hist(figsize=(20, 20), bins=50)
plt.show()

# %%
# Countplot of the target variable
data.reset_index(drop=True, inplace=True)
sns.countplot(data['needs_charge'])
plt.xlabel('Needs Charge')
plt.ylabel('Count')
plt.title('Distribution of Needs Charge')
plt.show()

# %%
# Pairplot of the features and target variable
sns.pairplot(data[features + ['needs_charge']], hue='needs_charge')
plt.show()

# %%
# Boxplots for each feature
plt.figure(figsize=(20, 10))
sns.boxplot(data=data[features])
plt.title('Boxplot of Features')
plt.xticks(rotation=90)
plt.show()

# %%
# Scatter plots
plt.figure(figsize=(10, 6))
sns.scatterplot(data=data, x='current_speed', y='battery_life', hue='needs_charge')
plt.title('Scatter Plot of Current Speed vs Battery Life')
plt.show()

# %%
# Violin plots
plt.figure(figsize=(10, 6))
sns.violinplot(x=data['needs_charge'], y=data['current_speed'])
plt.title('Violin Plot of Current Speed vs Needs Charge')
plt.show()

# %%
# cluster plot
from sklearn.cluster import KMeans

# Fit the KMeans algorithm to the data
kmeans = KMeans(n_clusters=3)
data['cluster'] = kmeans.fit_predict(data[features])

plt.figure(figsize=(10, 6))
sns.scatterplot(data=data, x='current_speed', y='battery_life', hue='cluster')
plt.title('Cluster Plot of Current Speed vs Battery Life')
plt.show()

# %%
# Dimensionality reduction plot
from sklearn.decomposition import PCA

# Apply PCA to the data
pca = PCA(n_components=2)
pca_result = pca.fit_transform(data[features])
data['pca_1'] = pca_result[:, 0]
data['pca_2'] = pca_result[:, 1]

plt.figure(figsize=(10, 6))
sns.scatterplot(data=data, x='pca_1', y='pca_2', hue='needs_charge')
plt.title('PCA Plot')
plt.show()

# %%
# Scatter plot
plt.figure(figsize=(10, 6))
sns.scatterplot(data=data, x='location', y='current_speed', hue='needs_charge')
plt.title('Current Speed vs Location')
plt.xlabel('Location (km)')
plt.ylabel('Current Speed (km/h)')
plt.show()

# %%
plt.figure(figsize=(15, 7))
plt.plot(data['location'], data['current_speed'], label='Current Speed')
plt.plot(data['location'], data['battery_life'], label='Battery Life')
plt.title('Current Speed and Battery Life along the Route')
plt.xlabel('Location (km)')
plt.ylabel('Value')
plt.legend()
plt.show()

# %% [markdown]
# Random Forest Classifier

# %%
# Build the model
from sklearn.ensemble import RandomForestClassifier

# %%
# Define the parameter grid
param_grid = {
    'n_estimators': [100, 200, 300, 400],
    'max_depth': [10, 20, 30, 40],
    'min_samples_split': [2, 5, 10],
    'min_samples_leaf': [1, 2, 4]
}

# %%
# Create a based model
rf = RandomForestClassifier(random_state=42)

# %%
# Instantiate the grid search model
from sklearn.model_selection import GridSearchCV


grid_search = GridSearchCV(estimator = rf, param_grid = param_grid, 
                          cv = 3, n_jobs = -1, verbose = 2)

# %%
# Fit the grid search to the data
grid_search.fit(X_train, y_train)

# %%
# Get the best parameters
best_params = grid_search.best_params_

print("Best Parameters: ", best_params)

# %%
# Train the model with the best parameters
model = grid_search.best_estimator_

# %%
# Evaluate the model
from sklearn.metrics import accuracy_score, classification_report

# %%
y_pred = model.predict(X_test)

# %%
accuracy = accuracy_score(y_test, y_pred)
print(f'Accuracy: {accuracy:.2f}')

# %%
print(classification_report(y_test, y_pred))

# %%
# Evaluate the model
from sklearn.metrics import accuracy_score, classification_report

y_pred = model.predict(X_test)

accuracy = accuracy_score(y_test, y_pred)
print(f'Accuracy: {accuracy:.2f}')

print(classification_report(y_test, y_pred))

# %%
# %%
from sklearn.model_selection import cross_val_score

# Use cross_val_score function
# Cross-Validation
scores = cross_val_score(model, X, y, cv=10, scoring='accuracy')
print('Cross-Validation Accuracy Scores', scores)
print('Average Cross-Validation Accuracy', scores.mean())


# %%
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense

# %%
# Initialize the constructor
model = Sequential()

# %%
# Add an input layer 
model.add(Dense(12, activation='relu', input_shape=(len(X.columns),)))

# %%
# Add one hidden layer 
model.add(Dense(8, activation='relu'))

# %%
# Add an output layer 
model.add(Dense(1, activation='sigmoid'))

# %%
# Compile the model
model.compile(loss='binary_crossentropy',
              optimizer='adam',
              metrics=['accuracy'])

# %%
model.summary()

# %%
# Define the callbacks
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint

callbacks = [EarlyStopping(patience=10, restore_best_weights=True),
             ModelCheckpoint(filepath='best_model.h5', save_best_only=True)]

# %%
# Fit the model
model.fit(X_train, y_train, epochs=100, batch_size=32, verbose=1, 
          validation_data=(X_test, y_test), callbacks=callbacks)

# %%
# Evaluate the model
score = model.evaluate(X_test, y_test, verbose=1)

print('Test loss:', score[0])
print('Test accuracy:', score[1])

# %%
def predict_need_charge(model, scaler, features):
    # print feature names and their values
       
    # scale the features
    features_scaled = scaler.transform(np.array(features).reshape(1, -1))
    # make prediction
    prediction = model.predict(features_scaled)
    return int(prediction.round())

features = [
    50,  # current_speed
    1.2,  # battery_capacity
    56,  # charge
    0.1,  # consumption
    100,  # distance_covered
    20,  # battery_life
    5,   # distance_to_charging_point
    0    # emergency_duration
]

prediction = predict_need_charge(model, scaler, features)
if prediction == 1:
    print("The car needs to charge in the next station.")
else:
    print("The car does not need to charge in the next station.")

# %% [markdown]
# 

# %%



