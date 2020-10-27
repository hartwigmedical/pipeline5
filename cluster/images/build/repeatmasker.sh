sudo apt-get -y install perl-modules cpanminus python3-h5py
sudo cpanm install Text::Soundex
## RepeatMasker
# Tandem Repeat Finder
sudo mkdir -p /opt/tools/trf/4.0.9
wget https://github.com/Benson-Genomics-Lab/TRF/releases/download/v4.09.1/trf409.linux64
sudo mv trf409.linux64 /opt/tools/trf/4.0.9/trf
sudo chmod a+x /opt/tools/trf/4.0.9/trf
# RMBlast
sudo mkdir -p /opt/tools/rmblast/2.10.0
wget http://www.repeatmasker.org/rmblast-2.10.0+-x64-linux.tar.gz
tar zxvf rmblast-2.10.0+-x64-linux.tar.gz
sudo mv mblast-2.10.0/bin/* /opt/tools/rmblast/2.10.0/
# HMMER
sudo mkdir -p /opt/tools/hmmer/3.3
wget http://eddylab.org/software/hmmer/hmmer-3.3.tar.gz
tar zxvf hmmer-3.3.tar.gz
cd hmmer-3.3
./configure -prefix=/opt/tools/hmmer/3.3 && make
sudo make install
cd -
# RepeatMasker
sudo mkdir -p /opt/tools/repeatmasker
wget http://www.repeatmasker.org/RepeatMasker-4.1.1.tar.gz
sudo tar zxvf RepeatMasker-4.1.1.tar.gz -C /opt/tools/repeatmasker/ 
sudo mv /opt/tools/repeatmasker/RepeatMasker /opt/tools/repeatmasker/4.1.1
cd /opt/tools/repeatmasker/4.1.1
sudo perl configure -default_search_engine rmblast -rmblast_dir /opt/tools/rmblast/2.10.0 -trf_prgm /opt/tools/trf/4.0.9/trf -hmmer_dir /opt/tools/hmmer/3.3
cd -
# create the human library cache
echo "AAAAAAAAAAAAAAAAAAAAA" > rmcache.fa
sudo /opt/tools/repeatmasker/4.1.1/RepeatMasker -species human rmcache.fa
sudo rm -r rmcache.fa*
cd /opt/tools/repeatmasker/4.1.1
sudo tar -cvf repeatmasker.tar *
cd -