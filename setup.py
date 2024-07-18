from setuptools import setup, find_packages

setup(
    name='dagster_project',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'dagster',
        'pyspark',
        'dagster-webserver',
        'dagster-pyspark',
        'pandas',
        'pyarrow' ,
        'dagster_aws'
    ],
    entry_points={
        'console_scripts': [
            'dagster_project=dagster_module.pipelines:defs',
        ],
    },
)
