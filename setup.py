from setuptools import setup, find_packages

VERSION = '0.0.1' 
DESCRIPTION = 'Spark Stream Monitoring package'
LONG_DESCRIPTION = 'This package can be used as a SparkStreamingListener to write logs about streaming queries so, these logs can be analyzed and used for monitoring and alerting purposes'

# Setting up
setup(
        name="spark_stream_monitoring", 
        version=VERSION,
        author="Ahmed Farag",
        author_email="a.farag033@incorta.com>",
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        packages=find_packages(),
	setup_requires=["wheel"],
        install_requires=[
            'pyspark>=3.4.0'
        ],
        keywords=[
            'python', 
            'spark', 
            'streaming', 
            'monitoring'
        ],
        classifiers= [
            "Development Status :: 3 - Alpha",
            "Programming Language :: Python :: 3 :: Only",
            "Operating System :: OS Independent",
        ]
)
