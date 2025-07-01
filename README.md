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

## Citing sdqlpy

To cite sdqlpy, use one of the following BibTex:

```
@inproceedings{DBLP:conf/cc/ShahrokhiS23,
  author       = {Hesam Shahrokhi and
                  Amir Shaikhha},
  editor       = {Clark Verbrugge and
                  Ondrej Lhot{\'{a}}k and
                  Xipeng Shen},
  title        = {Building a Compiled Query Engine in Python},
  booktitle    = {Proceedings of the 32nd {ACM} {SIGPLAN} International Conference on
                  Compiler Construction, {CC} 2023, Montr{\'{e}}al, QC, Canada,
                  February 25-26, 2023},
  pages        = {180--190},
  publisher    = {{ACM}},
  year         = {2023},
  url          = {https://doi.org/10.1145/3578360.3580264},
  doi          = {10.1145/3578360.3580264},
  timestamp    = {Sun, 19 Jan 2025 13:28:01 +0100},
  biburl       = {https://dblp.org/rec/conf/cc/ShahrokhiS23.bib},
  bibsource    = {dblp computer science bibliography, https://dblp.org}
}
```

```
@inproceedings{DBLP:conf/sigmod/ShahrokhiGYS23,
  author       = {Hesam Shahrokhi and
                  Callum Groeger and
                  Yizhuo Yang and
                  Amir Shaikhha},
  editor       = {Sudipto Das and
                  Ippokratis Pandis and
                  K. Sel{\c{c}}uk Candan and
                  Sihem Amer{-}Yahia},
  title        = {Efficient Query Processing in Python Using Compilation},
  booktitle    = {Companion of the 2023 International Conference on Management of Data,
                  {SIGMOD/PODS} 2023, Seattle, WA, USA, June 18-23, 2023},
  pages        = {199--202},
  publisher    = {{ACM}},
  year         = {2023},
  url          = {https://doi.org/10.1145/3555041.3589735},
  doi          = {10.1145/3555041.3589735},
  timestamp    = {Sun, 19 Jan 2025 13:27:26 +0100},
  biburl       = {https://dblp.org/rec/conf/sigmod/ShahrokhiGYS23.bib},
  bibsource    = {dblp computer science bibliography, https://dblp.org}
}
```
