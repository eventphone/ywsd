from setuptools import setup
import os

this_dir = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(this_dir, "README.md"), "r") as f:
    long_description = f.read()

setup(
    name="ywsd",
    version="0.14.1",
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
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3.14",
        "License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)",
    ],
    install_requires=[
        "aiopg",
        "aiohttp",
        "python-yate>=0.4",
        "pyyaml",
        "sqlalchemy==1.4.*",
    ],
    entry_points={
        "console_scripts": [
            "ywsd_init_db=ywsd.objects:main",
            "ywsd_engine=ywsd.engine:main",
            "ywsd_busy_cache=ywsd.busy_cache:main",
        ],
    },
    extra_require={"redis": ["redis[hiredis]"]},
)
