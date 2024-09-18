from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("airflow_common/VERSION", "r") as version_file:
    version = version_file.read().strip()

# Read the dependencies from base.txt
with open("requirements/base.txt") as f:
    install_requires = f.read().splitlines()
# Remove the constraints line from install_requires
install_requires = [line for line in install_requires if not line.startswith("-c")]

setup(
    name="airflow-common",
    version=version,
    description="Python common library used in Tapad",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="team-dgx",
    author_email="dgx-dev@tapad.com",
    url="https://github.com/Tapad/python-common",
    packages=find_packages(include=["airflow_common*"]),
    package_data={"airflow_common": ["**/*.json"]},
    python_requires=">=3.7",
    install_requires=install_requires,
    extras_require={
        "kfp": ["kfp"],
        "pandas": ["pandas>=1.0", "db-dtypes"],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
