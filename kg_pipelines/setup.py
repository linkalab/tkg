from setuptools import find_packages, setup

setup(
    name="kg_tourism",
    version="dev",
    author="luca secchi",
    author_email="luca.secchi@linkalab.it",
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Operating System :: OS Independent",
    ],
    packages=find_packages(exclude=["test"]),
    package_data={"kg_tourism": []},
    install_requires=[
        "dagster",
        "dagster-aws",
        "dagster-pandas",
        # "dagster-pyspark", ## uncomment to use spark dataframes
        "dagster-slack",
        "dagster-postgres",
        "mock",
        "pandas",
        "pyarrow",
        "requests",
        "fsspec",
        "s3fs",
        "scipy",
        "sklearn",
    ],
    extras_require={"tests": ["mypy", "pylint", "pytest"]},
)
