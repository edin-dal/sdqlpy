# sdqlpy: A Compiled Query Engine for Python

This project is an implementation of the research paper <b>"Building a Compiled Query Engine in Python"</b> that will be presented at the Compiler Construction Conference (<a href="https://conf.researchr.org/home/CC-2023">CC'23</a>) 

Use the following procedure to install <b>sdqlpy</b> and run its TPCH test suite:

### Required Linux packages:
```
sudo su
apt-get install libssl-dev openssl  
apt install libtbb-dev  
```

### Required MacOS packages:
```
brew install openssl  
brew install tbb@2020  
```
Note: do the specified exports after TBB installation.  

### Python building and installation:
This project uses the Python <b>ast</b> library and needs Python <b>version 3.8.10</b> to be installed on your machine. If you already have this version, ignore this part but make sure that `python3` correctly points to the above-mentioned version.  
```
cd ~  
mkdir tmp  
cd tmp  
wget https://www.python.org/ftp/python/3.8.10/Python-3.8.10.tgz  
tar zxvf Python-3.8.10.tgz  
cd Python-3.8.10  
./configure  
make  
make install  
alias python3=/usr/bin/python3.8  
```
### Python dependencies installation:
```
python3 -m pip install pip  
pip3 install numpy==1.22.0  
```
### Making the datasets ready:
```
mkdir sdqlpy_test  
cd sdqlpy_test  
git clone https://github.com/edin-dal/tpch-dbgen
cd tpch-dbgen  
make  
```
Replace the `1` parameter in the following command with the needed scaling factor:  
```
./dbgen -s 1 -vf  
export TPCH_DATASET=$PWD'/'  
```
### Installation of sdqlpy:
```
git clone https://github.com/edin-dal/sdqlpy  
cd sdqlpy/src  
python3 setup.py build  
python3 setup.py install  
```
### Testing TPCH queries:
```
cd ../test  
```
On Linux, sdqlpy enables <b>Hyper-Threading</b> by default. If you want to disable it you need to set the related environment variable to `1` as follows: (on MacOS it must also be set to `1` and user can decide about enabling/disabling it on their machine)
```
export NO_HYPER_THREADING=1
```
Inside <b>"test_all.py"</b>, configure the <b>sdqlpy_init</b> and the other parameters as explained in the internal comments. Then execute the file as follows:
```
python3 test_all.py  
```
### Notes:
- The project is still in the development phase and might have portability issues. Please kindly send us the issues you face while using this code.
- TBB and <a href="https://github.com/greg7mdp/parallel-hashmap">phmap</a> library are heavily used in the auto-generated C++ codes.