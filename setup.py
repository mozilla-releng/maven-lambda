import os
import re
from setuptools import setup, find_packages


project_dir = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(project_dir, 'version.txt')) as f:
    version = f.read().rstrip()

# We use the .in file because a library shouldn't pin versions, it breaks consumers' updates.
# We allow commented lines in this file
with open(os.path.join(project_dir, 'requirements.txt.in')) as f:
    requirements_raw = f.readlines()

requirements_without_comments = [
    line for line in requirements_raw if line and not line.startswith('#')
]

setup(
    name='maven-lambda',
    version=version,
    description="""Generate maven metadata on an S3 bucket thanks to an AWS Lambda function""",
    author='Mozilla Release Engineering',
    author_email='release+python@mozilla.com',
    url='https://github.com/mozilla-releng/maven-lambda',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    license='MPL2',
    install_requires=requirements_without_comments,
    classifiers=(
        'Programming Language :: Python :: 3',
    ),
)
