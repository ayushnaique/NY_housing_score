# Variables
HADOOP_CLASSPATH := `hadoop classpath`
JAR_NAME := ProfilingCleaning.jar
PRF_DIR := profiling
PRE_DIR := preprocessing
CLN_DIR := cleaning
BUILD_DIR := build

# Default target
all: clean compile jar

# Run all at once
run: all shift preprocess move shoot house clean_shoot clean_house

# Shift data to hadoop
shift:
	hadoop fs -mkdir NY_Housing
	hadoop fs -put Housing_Maintenance_Code_Violations_20000.csv NY_Housing

# Clean all
clean_all: clean purge

# Compile Java files and store .class files in the build directory
compile:
	mkdir -p $(BUILD_DIR)
	javac -classpath $(HADOOP_CLASSPATH) -d $(BUILD_DIR) $(PRF_DIR)/*.java $(PRE_DIR)/*.java $(CLN_DIR)/*.java

# Create JAR file from the compiled .class files
jar: compile
	jar cvf $(JAR_NAME) -C $(BUILD_DIR) .

# Run MapReduce job for preprocessing
preprocess: 
	hadoop jar $(JAR_NAME) preprocessing.HousingPreprocessing NY_Housing/Housing_Maintenance_Code_Violations_20000.csv NY_Housing/Housing_Violations

move:
	# Move the output back, append Index column and Push for profiling
	hadoop fs -get NY_Housing/Housing_Violations_preprocessed/part-r-00000 .
	# Append INDEX column to the preprocessed file
	awk 'BEGIN {OFS=","} NR==1 {print "INDEX," $$0} NR>1 {print NR-1 "," $$0}' part-r-00000 > Housing_Violations_preprocessed.csv
	rm part-r-00000
	hadoop fs -put Housing_Violations_preprocessed.csv NY_Housing/
	# Do the same for NYPD_shooting_data
	awk 'BEGIN {OFS=","} NR==1 {print "INDEX," $$0} NR>1 {print NR-1 "," $$0}' NYPD_Shooting_Incident_Data__Historic__20250417.csv > Shooting_Data_preprocessed.csv
	hadoop fs -put Shooting_Data_preprocessed.csv NY_Housing/

# Run MapReduce job for shooting data
shoot: 
	hadoop jar $(JAR_NAME) profiling.DataProfilerCSV NY_Housing/Shooting_Data_preprocessed.csv NY_Housing/NYPD_Shooting

# Run MapReduce job for housing data
house: 
	hadoop jar $(JAR_NAME) profiling.HousingProfilerCSV NY_Housing/Housing_Violations_preprocessed.csv NY_Housing/Housing_Violations

# Run MapReduce job for cleaning shooting data
clean_shoot: 
	hadoop jar $(JAR_NAME) cleaning.CleanerShootingData NY_Housing/Shooting_Data_preprocessed.csv NY_Housing/NYPD_Shooting

# Run MapReduce job for cleaning housing data
clean_house:
	hadoop jar $(JAR_NAME) cleaning.CleanerHousingData NY_Housing/Housing_Violations_preprocessed.csv NY_Housing/Housing_Violations

# Clean up compiled files and JAR
clean:
	rm -rf $(BUILD_DIR) $(JAR_NAME)

# Purge HDFS folders
purge:
	hdfs dfs -rm -r -skipTrash NY_Housing/

