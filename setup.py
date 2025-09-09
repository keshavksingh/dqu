from setuptools import setup, find_packages

setup(
    name="dqu",
    version="0.2.3",
    description="dqu is a data quality assesement framework supporting Spark, Flink, Ray and Pandas dataframes that empowers data and machine learning data developers to perform data quality assessments. dqu provides the data quality tool you need with enterprise-grade reliability and flexibility for Flink, Microsoft Fabric, Microsoft Synapse, Microsoft Azure ML, Azure Databricks, Jupyter Notebook data and ML piplines on Ray for Python/PySpark/PyFlink environments.",
    author="Keshav Kant Singh",
    author_email="keshav_singh@hotmail.com",
    packages=find_packages(include=["dqu", "dqu.*"]),
    install_requires=[
        #"pyspark>=3.0.0",
        #"pandas>=1.3.0",
        #"apache-flink>=2.1.0",
        #"ray>=2.48.0",
        #"scipy>=1.16.1",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
