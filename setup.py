from setuptools import setup

classifiers = [
    "Development Status :: 1 - Planning",
    "License :: OSI Approved :: Apache Software License",
    "Natural Language :: English",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
]

with open("README.md") as f:
    long_description = f.read()

with open("requirements.txt") as f:
    requirements = f.readlines()
install_requires = [r.strip() for r in requirements]

with open("dev-requirements.txt") as f:
    dev_requirements = f.readlines()
install_requires_dev = [r.strip() for r in dev_requirements]

install_requires = install_requires + install_requires_dev

setup(
    name="lasso",
    version="0.0.1",
    description="",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/wsp-sag/client_met_council_wrangler_utilities",
    license="Apache 2",
    platforms="any",
    packages=["lasso"],
    install_requires=install_requires,
)
