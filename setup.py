from setuptools import setup
import os

this_dir = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(this_dir, "README.md"), "r") as f:
    long_description = f.read()

setup(
    name="ywsd",
    version="0.9.16",
    packages=["ywsd"],
    url="https://gitlab.rc5.de/eventphone/ywsd",
    license="AGPLv3+",
    author="Garwin (Martin Lang)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author_email="garwin@eventphone.de",
    description="A yate routing engine for event telephone networks",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)",
    ],
    install_requires=[
        "aiopg",
        "aiohttp",
        "python-yate",
        "pyyaml",
        "sqlalchemy",
    ],
    entry_points={
        "console_scripts": [
            "ywsd_init_db=ywsd.objects:main",
            "ywsd_engine=ywsd.engine:main",
        ],
    },
)
