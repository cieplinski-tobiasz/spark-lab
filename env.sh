if [ -z "$JAVA_HOME" ]
then
    echo "JAVA_HOME environment variable is not set. Java 8 is required."
    return 1
fi

export SPARK_HOME="$PWD/spark-2.4.3-bin-hadoop2.7"
export PATH="$SPARK_HOME/bin:$PATH"
export SPARK_LAB_HOME=$PWD
