# Databricks notebook source
# import following libraries into databricks cluster: Keras tensorflow

# TensorFlow and tf.keras
import tensorflow as tf
from tensorflow import keras

# Helper libraries
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

print(tf.__version__)

# COMMAND ----------

#Get Mnist dataset- Keras already contains and divide it into Test and Train datasets
mnist = keras.datasets.mnist
(train_images, train_labels), (test_images, test_labels) = mnist.load_data()

# COMMAND ----------

#explore the data
sns.countplot(train_labels)



# COMMAND ----------

#explore the data
sns.countplot(test_labels)

# COMMAND ----------

#explore the data
print(train_images.shape)
print(test_images.shape)

# COMMAND ----------

#explore the data
plt.figure()
plt.imshow(train_images[10])
display(plt.show())
print(train_labels[10])

# COMMAND ----------

#explore the data
plt.figure(figsize=(5,10))
for i in range(25):
    plt.subplot(5,5,i+1)
    plt.xticks([])
    plt.yticks([])
    plt.grid(False)
    plt.imshow(train_images[i], cmap=plt.cm.binary)
    plt.xlabel(train_labels[i])
display(plt.show())    #in databricks plt. show not enough

# COMMAND ----------

#Prepare/ modify the data
train_images = train_images / 255.0
test_images = test_images / 255.0

# COMMAND ----------

model = keras.Sequential([
    keras.layers.Flatten(input_shape=(28, 28)),
    #keras.layers.Dropout(0.2),
    #keras.layers.Dense(100, activation=tf.nn.relu),
    #keras.layers.Dense(100, activation=tf.nn.relu),
    #keras.layers.Dropout(0.2),
    keras.layers.Dense(100, activation=tf.nn.relu),
    #keras.layers.Dropout(0.2),
    keras.layers.Dense(100, activation=tf.nn.relu),
    #keras.layers.Dropout(0.2),
    keras.layers.Dense(10, activation=tf.nn.softmax)
])

# COMMAND ----------

#compile the model
model.compile(optimizer='adam', 
              loss='sparse_categorical_crossentropy',
              metrics=['accuracy'])


# COMMAND ----------

#Train the model
model.fit(train_images, train_labels, epochs=5)

# COMMAND ----------

#test the model
test_loss, test_acc = model.evaluate(test_images, test_labels)
print('Test accuracy:', test_acc)

# COMMAND ----------

#test the model
image = test_images[28]
plt.figure()
plt.imshow(image)
display(plt.show())

# COMMAND ----------

#test the model
#one image as an array
print(image.shape)
image = (np.expand_dims(image, 0))
print(image.shape)  # (1 x 28 x 28)

# COMMAND ----------

#test the model
predictions = model.predict(image)
print(predictions)

# COMMAND ----------

#test the model
value=np.argmax(predictions)
prob=np.max(predictions)
print(value)
print(prob)

# COMMAND ----------



# COMMAND ----------

