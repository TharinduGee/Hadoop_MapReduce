
# Analyzing Bike-Sharing System Usage Patterns (Hadoop MapReduce)

This project analyzes large-scale bike-sharing trip data from the Citi Bike system in New York City to identify the most popular stations and determine the busiest hours for these stations. It leverages the distributed processing capabilities of **Hadoop MapReduce**.

## 📁 Dataset

- **Source**: [Citi Bike NYC System Data](https://citibikenyc.com/system-data)
- **Format**: Monthly CSV files
- **Year Used**: 2023
- **Total Records**: ≈ **35,107,070**

Each record includes trip start and end times, station information, bike types, and user types.

## 🛠️ Prerequisites

- Ubuntu (or WSL)
- Java 8 (JDK)
- Hadoop 3.3.6 (pseudo-distributed mode)
- Git

## ⚙️ Setup Instructions

### Java Installation

```bash
sudo apt update
sudo apt install openjdk-8-jdk
```

### Hadoop Setup

1. Download and extract Hadoop:
   ```bash
   wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
   tar -xzf hadoop-3.3.6.tar.gz
   ```

2. Configure environment variables in `~/.bashrc`:
   ```bash
   export HADOOP_HOME=~/hadoop-3.3.6
   export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
   export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$JAVA_HOME/bin
   ```

3. Configure Hadoop XML files:
   - `core-site.xml`
   - `hdfs-site.xml`
   - `mapred-site.xml`
   - `yarn-site.xml`

4. Format the HDFS NameNode:
   ```bash
   hdfs namenode -format
   ```

5. Start Hadoop services:
   ```bash
   start-dfs.sh
   start-yarn.sh
   ```

## 🚴‍♂️ Dataset Preparation

1. Download Citi Bike data for 2023 from [tripdata S3](https://s3.amazonaws.com/tripdata/index.html)
2. Upload to HDFS:
   ```bash
   hdfs dfs -mkdir popular_stations_input
   hdfs dfs -put /path/to/data/*.csv popular_stations_input
   ```

## 🧩 MapReduce Jobs

### 🔍 Job 1 – Identify Popular Stations

#### Compile

```bash
export HADOOP_CLASSPATH=$(hadoop classpath)
mkdir classes_popular_stations
javac -cp $HADOOP_CLASSPATH -d classes_popular_stations PopularStationsDriver.java PopularStationsMapper.java PopularStationsReducer.java
jar -cvf popularStations.jar -C classes_popular_stations .
```

#### Run

```bash
hadoop jar popularStations.jar PopularStationsDriver popular_stations_input popular_stations_output
```

### 📊 Get Top 10 Stations

```bash
hdfs dfs -cat popular_stations_output/part-r-00000 | sort -t$'	' -k2,2nr | head -n 10 | awk -F'	' '{print $1}' > /tmp/top_stations.txt
hdfs dfs -mkdir -p job2_cache_data
hdfs dfs -put /tmp/top_stations.txt job2_cache_data/top_stations.txt
```

---

### ⏰ Job 2 – Find Busiest Hours

#### Compile

```bash
mkdir classes_busiest_hour
javac -cp $HADOOP_CLASSPATH -d classes_busiest_hour BusiestHourDriver.java BusiestHourMapper.java BusiestHourReducer.java
jar -cvf busiestHour.jar -C classes_busiest_hour .
```

#### Run

```bash
hadoop jar busiestHour.jar BusiestHourDriver popular_stations_input busiest_hour_output job2_cache_data/top_stations.txt
```

### 📈 View Results

```bash
hdfs dfs -cat busiest_hour_output/part-r-00000 | sort -t$'	' -k2,2nr | head -n 10
```

## 📌 Future Enhancements

- Integration with visualization tools (Power BI, Grafana)
- Automated job scheduling with cron or Apache Airflow
- Incremental processing with partitioned datasets

## 📚 Authors

- Galpayage G. D. T. G. – EG/2020/3935  
- Vihanga V. M. B. P. – EG/2020/4252
- Weerasekara W.M.N.S. - EG/2020/4266
- University of Ruhuna, Group 35
