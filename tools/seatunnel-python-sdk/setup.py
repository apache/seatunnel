from setuptools import setup, find_packages

setup(
    name='seatunnel',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'httpx'
    ],
)