import os
from setuptools import setup

setup(
    name = "sdqlpy",
    version = "1.0.0",
    author = "Hesam Shahrokhi",
    author_email = "hesam.shahrokhi@ed.ac.uk",
    description = ("A Compiled Query Engine for Python"),
    license = "-",
    keywords = "-",
    url = "-",
    packages=["sdqlpy"],
    long_description='README.md',
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: BSD License",
    ],
    include_package_data=True,
)