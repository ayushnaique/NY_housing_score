# Variables
HADOOP_CLASSPATH := `hadoop classpath`
JAR_NAME := dataProfiling.jar
SRC_DIR := Data_profiling
CLASS_FILES := $(SRC_DIR)/*.class

# Default target
all: compile jar

# Compile Java files
compile:
    javac -classpath $(HADOOP_CLASSPATH) $(SRC_DIR)/*.java

# Create JAR file
jar: compile
    jar cvf $(JAR_NAME) $(CLASS_FILES)

# Run MapReduce job for shooting data
shoot: jar
    hadoop jar $(JAR_NAME) $(SRC_DIR)/DataProfilerCSV profiling/Shooting_Data_preprocessed.csv profiling/output

# Run MapReduce job for housing data
house: jar
    hadoop jar $(JAR_NAME) $(SRC_DIR)/HousingProfilerCSV housing_profiling/Housing_Violations_preprocessed.csv housing_profiling/output

# Clean up compiled files and JAR
clean:
    rm -f $(CLASS_FILES) $(JAR_NAME)

# Purge HDFS folders
purge_shoot:
    hdfs dfs -rm -r -skipTrash profiling/output_dates
    hdfs dfs -rm -r -skipTrash profiling/output_nulls
    hdfs dfs -rm -r -skipTrash profiling/output_missing_coords

purge_house:
    hdfs dfs -rm -r -skipTrash housing_profiling/output_dates
    hdfs dfs -rm -r -skipTrash housing_profiling/output_nulls
    hdfs dfs -rm -r -skipTrash housing_profiling/output_missing_coords