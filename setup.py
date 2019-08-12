import setuptools

with open("README.md") as fh:
    long_description = fh.read()

with open("VERSION") as version_file:
    version = version_file.read().strip()

setuptools.setup(
    name="talker",
    version=version,
    author="Doron Cohen",
    author_email="doron@weka.io",
    description="The almighty remote command executor for Unix machines",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/weka-io/talker",
    license='BSD',
    install_requires=[
        'redis==2.10.5',
        'weka-easypy==0.3.1'
    ],
    extras_require={
        "dev": ["ipython"],
        "test": ["fakeredis==0.10.1", "mock==3.0.5"],
    },
    packages=["talker"],
    classifiers=[
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Topic :: Communications",
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Operating System :: Unix",
        "Operating System :: MacOS",
        "License :: OSI Approved :: BSD License",
    ],
)
