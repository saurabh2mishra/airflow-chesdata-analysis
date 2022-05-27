import re

from setuptools import find_packages, setup

VERSION_FILE = "plugins/__init__.py"
with open(VERSION_FILE, "rt") as version_file:
    verstrline = version_file.read()
version_re = r"^__version__ = ['\"]([^'\"]*)['\"]"
mo = re.search(version_re, verstrline, re.M)
if mo:
    verstr = mo.group(1)
else:
    raise RuntimeError("Unable to find version string in %s." % (VERSION_FILE,))

setup(
    productname='CHES2019 Data Analysis',
    productversion='1.0',
    version=verstr,
    name='csdanalysis',
    packages=find_packages(include=["src"]),
    description='CHES2019 data analysis',
    author='Saurabh Mishra',
    author_email='saurabh2.mishra@gmail.com',
    license='MIT'
)