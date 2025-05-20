from setuptools import setup, find_packages

setup(
    name="taskpipe",
    version="0.3.0",
    description="A Python task processing library",
    author="kuaizhirui",
    author_email="kuaizhirui@gmail.com",
    packages=find_packages(),
    python_requires=">=3.7",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)