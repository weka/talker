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
        'redis==3.3.7',
        'weka-easypy==0.3.1'
    ],
    extras_require={
        "dev": ["ipython"],
        "test": [
            "fakeredis==1.0.4", "mock==3.0.5",
            # we need to install plumbum in order to make easypy functions -
            # 'initialize_exception_listener' and 'raise_in_main_thread' work properly in client's test
            "plumbum@git+https://github.com/weka-io/plumbum@master#egg=plumbum"
        ],
    },
    packages=["talker", "talker_agent"],
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
