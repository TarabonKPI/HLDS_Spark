# Spark Taxi Data Generator

Generates a London taxi data based on multiple datasets.

By: Team#8 - Goncharuk, Kyrychenko, Trubaichuk

## Generated dataset schema:
  * String id;
  * String driverName;
  * String clientName;
  * String pickupPostcode;
  * String pickupLongitude;
  * String pickupAltitude;
  * String dropoffPostcode;
  * String dropoffLongitude;
  * String dropoffAltitude;
  * LocalDateTime pickupDateTime;
  * LocalDateTime dropoffDateTime;
  * float cost;
  * int driverRating;
  * String driverComment;
  * int clientRating;
  * String clientComment;


## How It Works

1. `Data Multiplication`
   Multiplies a number of rows in the datasets by a given factor.
   For each dataset:
    1.   Calculate the correlation factor in respect to the main dataset
    2.   If correlation_factor < 1 : multiply dataset by **correlation_factor * global_factor**
   The point is to align number of rows of each dataset in respect to the main dataset - it allows to generate a full dataset with minimal number of NaN values.
2. `Data Shuffling and Identity Provisioning`
   Shuffle entities in the datasets. Assign an unique **id** for each entity of the dataset.
   Operation allows to generate a huge dataset with minimal number of identical entities.
3. `Cost Data Generation`
   Generate random *cost* value for each of the entities in range **3...20** euro - a median cost of taxi cab in London.
4. `Join operation on multiple datasets`
   Perform *LEFT join* operation on multiple datasets. Number of entities in each of the datasets is approximately equal, so there is no big data loss. Drop *id*    column to avoid *mutiple identical columns* exceptions - there is no need in the column anymore due to complete join operation.


## Reports

The application has been tested and evaluated on both local machine (Java Spark) and Google Colab environment (Pyspark).
Due to small computational power of the local machine the performance testing task has been performed in Google Colab.

Results:

1. Number of rows in the generated dataset: 31474600
2. Size: 1.9G
3. Time spent to generate the dataset: 43.73 seconds

## "Anatomical" example
[<-click me to see the Colab Environment->](https://colab.research.google.com/drive/1D8Y5RLbuuxiJRp4LLXV6hvLyZDGpp7_2?usp=sharing)

