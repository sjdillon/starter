# starter
[![Build Status](https://travis-ci.org/sjdillon/starter.svg?branch=master)](https://travis-ci.org/sjdillon/starter)

**starter** a simple project to copy to get started with other projects

* `starter_class.py` -- example class and helper functions for querying athena
* `boto_manager.py` -- creates and manages boto sessions and clients, allows mocking playback and recording.  Used in the unit tests.

# What is it?
- This is a simple seed project to copy
- It has the basic structure to:
    - create an installable python package
    - run unit tests
    - run tox for static code analysis
    - run various CI tools (travis, gcp)
    - mock boto calls
    
# Requirements
- Python 3.7

# Quick start
Install starter

`$ pip install git+https://github.com/sjdillon/starter.git`

Set up credentials (in e.g. ~/.aws/credentials):

```
[default]
aws_access_key_id = YOUR_KEY
aws_secret_access_key = YOUR_SECRET`
```

set up a default region (in e.g. ~/.aws/config):
```buildoutcfg
[default]
region=us-east-1
```



# Run the tests
```
tox

======================================================================================================================= 4 passed in 0.35s ========================================================================================================================
____________________________________________________________________________________________________________________________ summary _____________________________________________________________________________________________________________________________
  py27: commands succeeded
  py3: commands succeeded
  py37: commands succeeded
  congratulations :)

```



# install 
locally
```buildoutcfg
pip install -e .
```
or from github
```buildoutcfg
pip install git+https://github.com/sjdillon/starter.git
```

# show classes
- see ``__init__.py`` for defining exposed classes
```buildoutcfg
>>$ python -c 'import starter; print(dir(starter))'
['BotoClientManager', 'StarterClass', '_CONFIG', '__all__', '__builtins__', '__cached__', '__doc__', '__file__', '__loader__', '__name__', '__package__', '__path__', '__spec__', 'boto_manager', 'config', 'starter_class']
```



