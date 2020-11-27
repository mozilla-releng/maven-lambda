# maven-lambda

[![Build Status](https://travis-ci.org/mozilla-releng/maven-lambda.svg?branch=master)](https://travis-ci.org/mozilla-releng/maven-lambda) [![Coverage Status](https://coveralls.io/repos/github/mozilla-releng/maven-lambda/badge.svg?branch=master)](https://coveralls.io/github/mozilla-releng/maven-lambda?branch=master)

Generate maven metadata on an S3 bucket thanks to an AWS Lambda function.

## Why this project?

The [mozilla-mobile](https://github.com/mozilla-mobile/) teams need a maven repository to host Mozilla's Android libraries, like Geckoview or Android-Components. A maven repository is a regular HTTP server that delivers binaries and metadata files. Nothing is custom in terms of protocol, a maven/gradle client just expects files to [follow a given tree structure](https://blog.packagecloud.io/eng/2017/03/09/how-does-a-maven-repository-work/).

Mozilla could have used an existing product like [Sonatype's Nexus](https://www.sonatype.com/nexus/). Although, we want to ensure the security of these binaries in case an attacker compromises the server. There are 2 main ways to solve this:

 a. sign the binaries to ensure they were modified at rest.

 b. monitor the server to make sure no unusual activity happens.

Option a. could be done thanks to GPG. The problem is: the maven/gradle ecosystem does not enforce binaries to be signed. It even forced the Signal Android App to pin dependencies hashes through their [in-house gradle plugin](https://github.com/signalapp/gradle-witness). So we have to fallback on option b.

Nexus is not a known product at Mozilla. We have no expertise in knowing it. Although, we have years of experience in fine-tuning AWS S3 buckets. So we decided host our maven instance there. It was easy to deploy binaries on S3 thanks to [beetmover](https://github.com/mozilla-releng/scriptworker-scripts/tree/master/beetmoverscript). The only thing missing was metadata, and more precisely `maven-metadata.xml`. That's what this project is about.

## Design choices

We had 2 choices to generate `maven-metadata.xml`: either a Taskcluster task generated it then `beetmover` uploaded it, or something closer to S3 handled it. The problem with the former is that we have a several minutes between the time it was generated and the time it was uploaded. Moreover, another concurrent task may overwrite changes and discard what was generated a few minutes before.

Thus, we decided to go with the second option which translates into an AWS Lambda function. A Lambda function is a small piece of code that gets run on an AWS event like "something just got uploaded on S3". [metadata.py](https://github.com/mozilla-releng/maven-lambda/blob/master/maven_lambda/metadata.py) is executed everytime a new binary is uploaded and regenerates the metadata based on what is already uploaded. No risk of generating outdated metadata.

## Hack on the code
```sh
virtualenv venv        # create the virtualenv in ./venv
. venv/bin/activate    # activate it
git clone https://github.com/mozilla-releng/maven-lambda
cd maven-lambda
pip install maven-lambda
```

## Links

 * Production instance: https://maven.mozilla.org/
 * Staging instance: https://maven-default.stage.mozaws.net/

## Deployment

The process is quite manual. See [bug 1589065 comment 27](https://bugzilla.mozilla.org/show_bug.cgi?id=1589065#c27) for instance. The idea is:

 1. let Taskcluster build a push to the master branch
 2. Get the direct link to `function.zip`
 3. Needinfo CloudOps on Bugzilla with that direct link.

## Debugging

Contact the CloudOps team to get access to the logs. They may refer you to tools like [awslogs](https://github.com/jorgebastida/awslogs).
