# Variables
HADOOP_CLASSPATH := `hadoop classpath`
JAR_NAME := dataProfiling.jar
PRF_DIR := Data_profiling
PRE_DIR := preprocessing
BUILD_DIR := build

# Default target
all: clean compile jar

# Compile Java files and store .class files in the build directory
compile:
        mkdir -p $(BUILD_DIR)
        javac -classpath $(HADOOP_CLASSPATH) -d $(BUILD_DIR) $(PRF_DIR)/*.java $(PRE_DIR)/*.java

# Create JAR file from the compiled .class files
jar: compile
        jar cvf $(JAR_NAME) -C $(BUILD_DIR) .

# Run MapReduce job for preprocessing
preprocess: jar
        hadoop jar $(JAR_NAME) preprocessing.HousingPreprocessing housing_profiling/Housing_Violations_preprocessed.csv housing_profiling/output

# Run MapReduce job for shooting data
shoot: jar
        hadoop jar $(JAR_NAME) Data_profiling.DataProfilerCSV profiling/Shooting_Data_preprocessed.csv profiling/output

# Run MapReduce job for housing data
house: jar
        hadoop jar $(JAR_NAME) Data_profiling.HousingProfilerCSV housing_profiling/Housing_Violations_preprocessed.csv housing_profiling/output

# Clean up compiled files and JAR
clean:
        rm -rf $(BUILD_DIR) $(JAR_NAME)

# Purge HDFS folders
purge_shoot:
        hdfs dfs -rm -r -skipTrash profiling/output_dates
        hdfs dfs -rm -r -skipTrash profiling/output_nulls
        hdfs dfs -rm -r -skipTrash profiling/output_missing_coords

purge_house:
        hdfs dfs -rm -r -skipTrash housing_profiling/output_dates
        hdfs dfs -rm -r -skipTrash housing_profiling/output_nulls
        hdfs dfs -rm -r -skipTrash housing_profiling/output_missing_coords