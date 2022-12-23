# Cyberattack Classification using MQTT Dataset
**NAME:** Jong Hoon Park \


# Dataset
Shown below is the schema of the original MQTT dataset. The original data set consists of 20,000,000 rows of data with 34 columns, last column being the _target_.

**MQTT Dataset (Original)** \
 |-- tcp.flags: string (nullable = true) \
 |-- tcp.time_delta: string (nullable = true) \
 |-- tcp.len: string (nullable = true) \
 |-- mqtt.conack.flags: string (nullable = true) \
 |-- mqtt.conack.flags.reserved: string (nullable = true) \
 |-- mqtt.conack.flags.sp: string (nullable = true) \
 |-- mqtt.conack.val: string (nullable = true) \
 |-- mqtt.conflag.cleansess: string (nullable = true) \
 |-- mqtt.conflag.passwd: string (nullable = true) \
 |-- mqtt.conflag.qos: string (nullable = true) \
 |-- mqtt.conflag.reserved: string (nullable = true) \
 |-- mqtt.conflag.retain: string (nullable = true) \
 |-- mqtt.conflag.uname: string (nullable = true) \
 |-- mqtt.conflag.willflag: string (nullable = true) \
 |-- mqtt.conflags: string (nullable = true) \
 |-- mqtt.dupflag: string (nullable = true) \
 |-- mqtt.hdrflags: string (nullable = true) \
 |-- mqtt.kalive: string (nullable = true) \
 |-- mqtt.len: string (nullable = true) \
 |-- mqtt.msg: string (nullable = true) \
 |-- mqtt.msgid: string (nullable = true) \
 |-- mqtt.msgtype: string (nullable = true) \
 |-- mqtt.proto_len: string (nullable = true) \
 |-- mqtt.protoname: string (nullable = true) \
 |-- mqtt.qos: string (nullable = true) \
 |-- mqtt.retain: string (nullable = true) \
 |-- mqtt.sub.qos: string (nullable = true) \
 |-- mqtt.suback.qos: string (nullable = true) \
 |-- mqtt.ver: string (nullable = true) \
 |-- mqtt.willmsg: string (nullable = true) \
 |-- mqtt.willmsg_len: string (nullable = true) \
 |-- mqtt.willtopic: string (nullable = true) \
 |-- mqtt.willtopic_len: string (nullable = true) \
 |-- target: string (nullable = true)


Only 5% of this entire dataset was used to avoid out-of-memory error. There were several columns with single distinct values, and these columns have no effect on the dataset and can be removed. In addition, _"mqtt.msg"_ column was dropped due to high number of outliers, and _"mqtt.protoname"_ column was also dropped because this column is already represented by another column named _"mqtt.proto_len."_ Shown below is the schema of the cleaned dataset. This dataset is the input for the preprocessing pipeline in Spark. The pipeline transforms the above dataset to a table with assembed feature vectors and encoded target. Note that column names are also renamed to replace periods _(".")_.


**MQTT Dataset (Cleaned)** \
 |-- tcp_flags: string (nullable = true) \
 |-- tcp_time_delta: string (nullable = true) \
 |-- tcp_len: string (nullable = true) \
 |-- mqtt_conack_flags: string (nullable = true) \
 |-- mqtt_conack_val: string (nullable = true) \
 |-- mqtt_conflag_cleansess: string (nullable = true) \
 |-- mqtt_conflag_passwd: string (nullable = true) \
 |-- mqtt_conflag_uname: string (nullable = true) \
 |-- mqtt_conflags: string (nullable = true) \
 |-- mqtt_dupflag: string (nullable = true) \
 |-- mqtt_hdrflags: string (nullable = true) \
 |-- mqtt_kalive: string (nullable = true) \
 |-- mqtt_len: string (nullable = true) \
 |-- mqtt_msgid: string (nullable = true) \
 |-- mqtt_msgtype: string (nullable = true) \
 |-- mqtt_proto_len: string (nullable = true) \
 |-- mqtt_qos: string (nullable = true) \
 |-- mqtt_retain: string (nullable = true) \
 |-- mqtt_ver: string (nullable = true) \
 |-- target: string (nullable = true) \
 |-- Train_or_Test: integer (nullable = true)


# Machine Learning

### Spark ML

For Spark ML models, logistic regression and random forest classification algorithms were used to perform classification on MQTT dataset.

- Logistic Regression was chosen because it is easy to implement and is relatively cheap in terms of computation. It also provides decent accuracy. The hyperparameters tuned for this model were "maxIter" and "regParam" which represent the maximum number of training iteration and regularization coefficient. The optimal parameters were determined via cross-validation (CV), and the parameters were **maxIter = 30, regParam = 0.01**. In general, the model shows better performance as the number of iterations increase and regularization coefficient decreases unless the model is overfitted to the train data.

- Random Forest was chosen because it can be used for unbalanced dataset as it deals with the subset of features while growing trees. The hyperparameters tuned were "maxBins" and "numTrees." They were also determined via CV and the results were **maxBins = 64 and numTrees = 20**. The maxBins parameter indicates the number of maximum splits of data features, and the numTrees parameter indicates the number of individual "trees" in the random forest model. Random forest algorithm is also a solution to overfitting in decision tree algorithm.

---

### Tensorflow

Tensorflow and keras only allow to vary the layer setups of the model. The hyperparameters for the first model were NN_depth, NN_width, and optimizer. For the second model, the learning rate and activation function were chosen as hyperparameters, and the layer setup was fixed. After the cross validation, for the first model, the optimal hyperparameters were **depth = 2, width = 30, and optimizer = Adam**. For the second model, determined learning rate was **0.01**, and the activation function was rectified linear unit (**ReLU**).

Tensorflow provides convenient ways of saving and loading models. The best models from each iteration were saved as _best_model1.h5_ and _best_model2.h5_. The saved models were then loaded and evaluated to show that the model loading was successful. 



# Guide on Codes

## Files
The project consists of 4 iPython notebook files (.ipynb) and a PDF file with screenshots of the code output summary: \
- MQTT_kafka_consumer.ipynb
- MQTT_kafka_producer.ipynb
- MQTT_Local.ipynb
- MQTT_GCP.ipynb
- MQTT_Screenshots.pdf


The MQTT_Local file is to run on local machine environment, and the MQTT_GCP file is for running on Google Cloud Cluster environment. The difference between these two are just the path of the dataset. When using Google Cloud computing, csv files of the original dataset were uploaded to the batch. In the last part (Task IV) of the screenshots, successful connection to Postgres on GCP using Google Cloud SQL and terminal is shown. However, connecting the created Cloud Postgres DB to Jupyter Notebook was not successful. The MQTT_kafka_consumer and MQTT_kafka_producer files correspond to **Task II-4** of the project. Please check out the MQTT_Screenshots.pdf for summary.


## Notes
Make sure the csv files of the original data (train70_augmented.csv, test30_augmented.csv) are in the path shown below:

- train_path = "./mqtt_data/train70_augmented.csv"
- test_path = "./mqtt_data/test30_augmented.csv"

This can be done by adding the csv files to a folder named "mqtt_data" and placing the folder and the notebook files in the same directory.
Another way is just to redefine the train_path and test_path as desired.


### MQTT_Local.ipynb

- Under **Task I**, redefine train_path and test_path if need to.
- Edit _db_properties['password']_ to user's own password to access the local DB properly.
- Convert the postgres ingestion code cells (Task I and III) to Raw NBConvert to deactivate when run once as it can lead to multiple ingestions.
- Under **Task II**, replacing "df = df_read" with "df = reduced_df" lets you avoid the postgres ingestion/reading process and directly use the data frame created previously.
- Under **PySpark ML Models**, decreasing the number of parameters in the paramGrid can reduce runtime.
- Under **Model Evaluation on Test Dataset**, when using the _plot_confusion_matrix_ function, set the _normalize=True_ to see the normalized confusion matrix.
- Under **Tensorflow ML Models**, when using the _kfold_ function, _k_ can be set to a different user-defined value to split the datasets to k-fold.
- Similar to the PySpark model, decreasing the number of hyperparameters in _hp.HParam_ parameter grid will significantly reduce the runtime as shown for **Model 2**.
- After running the Tensorflow ML part, best models and the cross-validation results will be saved to a subdirectory names "mqtt_NN."


### MQTT_kafka_producer.ipynb (Task II-4)

- Make sure Apache Kafka is available. For this project, Docker was used to activate kafka in Python.
- Edit Twitter developer authentification keys (bearer_token, api_key, api_secret, access_token, and access_token_secret) to actually run the feed streaming.
- Converting the very bottom raw NBconvert cell to a code cell and running would delete all the stream rules previously defined. Use it to reset the stream rules.





