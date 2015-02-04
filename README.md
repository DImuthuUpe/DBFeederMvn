DBFeederMvn
===========

<h5>Building instructions</h5>

Clone new corpus tools and build using mvn clean install
https://github.com/madurangasiriwardena/corpus.sinhala.tools

Set root data folder path in Controller.java
Build project using mvn clean install

Install Oracle jar to local maven repository

mvn install:install-file -Dfile=ojdbc7.jar -DgroupId=ojdbc -DartifactId=ojdbc -Dversion=7 -Dpackaging=jar

ojdbc7.jar can be found from src/main/resources folder

<h5>Running Instructions</h5>

Go to target folder
java -jar DBFeederMvn-1.0.0-jar-with-dependencies.jar




