import os

from setuptools import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="relationalize",
    author="Henry Jones",
    author_email="henry.jones@tulip.co",
    url="https://github.com/tulip/relationalize",
    description="A utility for converting/transporting arbitrary JSON data into a relational database",
    packages=["relationalize"],
    package_dir={"relationalize": "relationalize"},
    package_data={"relationalize": ["py.typed"]},
    include_package_data=True,
    long_description=read("README.md"),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Topic :: Utilities",
    ],
)
