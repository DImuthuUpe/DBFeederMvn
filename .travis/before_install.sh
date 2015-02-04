echo "Building Local Dependencies"
mkdir local_dependencies
cd local_dependencies
echo "Cloning corpus.sinhala.tools"
git clone https://github.com/madurangasiriwardena/corpus.sinhala.tools
cd corpus.sinhala.tools
echo "Building corpus tools"
mvn clean install
echo "Adding Oracle Dependencies"
cp ../src/main/resources/ojdbc7.jar .
mvn install:install-file -Dfile=ojdbc7.jar -DgroupId=ojdbc -DartifactId=ojdbc -Dversion=7 -Dpackaging=jar
