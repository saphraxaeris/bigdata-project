# Big Data Analytics Class - Project

## Building JAR
* `cd` to project root
* Run `mvn package`

## Running Drivers
* To run all drivers use command `hadoop jar <jar location> <input file from hdfs> <output folder in hdfs> <true or false>`
* * Last parameter is to run data massager on generated data. This will cause the program to take a long time while it creates the necessary files. A pre-computed version is alreaady included with the project.

## Viewing Data
* Open 'index.html' in a browser
* Takes around 1-2 minutes to display all data