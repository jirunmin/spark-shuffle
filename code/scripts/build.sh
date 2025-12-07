#!/bin/bash

cd /home/spark/shuffle-app

echo ">>> Cleaning and packaging Spark project..."
sbt clean package

echo ""
echo ">>> Build complete!"
echo ">>> Generated JARs:"
ls -lh target/scala-2.10/*.jar
