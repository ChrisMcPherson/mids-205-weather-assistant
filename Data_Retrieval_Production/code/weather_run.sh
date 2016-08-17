export JAR_HOME=/weather/code/lib

for f in $JAR_HOME/*.jar
do
JAR_CLASSPATH=$JAR_CLASSPATH:$f
done
export JAR_CLASSPATH

#the next line will print the JAR_CLASSPATH to the shell.
echo the classpath $JAR_CLASSPATH

java -classpath $JAR_CLASSPATH:/weather/code/bin com.weather.Weather

find /weather/data -name "*"  -type d -mtime +1 -exec rm -fr "{}" \;
