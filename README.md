# Requirements

Spark requires Java 8 and Python 2.7+/3.4+.

# Setup

Make sure that `setup.sh` is executable

```
chmod a+x setup.sh
```

and run

```
source ./setup.sh
```

The script will download Spark and add it to PATH.
It will also set some sensible environment variables
and download an exemplary dataset.

# Usage

After the setup you should be able to run interactive shell by typing

```
pyspark
```

If you want to run an already written script, use this command:

```
spark-submit your_script.py
```
