echo "Building Local Dependencies"
mkdir local_dependencies
cd local_dependencies
echo "Cloning corpus.sinhala.tools"
git clone https://github.com/madurangasiriwardena/corpus.sinhala.tools
cd corpus.sinhala.tools
echo "Building corpus tools"
mvn clean install
