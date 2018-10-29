# maven-lambda

[![Build Status](https://travis-ci.org/mozilla-releng/maven-lambda.svg?branch=master)](https://travis-ci.org/mozilla-releng/maven-lambda) [![Coverage Status](https://coveralls.io/repos/github/mozilla-releng/maven-lambda/badge.svg?branch=master)](https://coveralls.io/github/mozilla-releng/maven-lambda?branch=master)

Generate maven metadata on an S3 bucket thanks to an AWS Lambda function.


## Hack on the code
```sh
virtualenv venv        # create the virtualenv in ./venv
. venv/bin/activate    # activate it
git clone https://github.com/mozilla-releng/maven-lambda
cd maven-lambda
pip install maven-lambda
```
