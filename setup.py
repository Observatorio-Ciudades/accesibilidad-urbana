from setuptools import find_packages, setup

setup(
    name="aup",
    version="0.1",
    packages=find_packages(exclude=["tests*"]),
    license="none",
    description="Package to run the analysis for urban accesibility.",
    long_description=open("README.md").read(),
    install_requires=[],
    url="https://github.com/Observatorio-Ciudades/accesibilidad-urbana",
    author="Observatorio de Ciudades",
    author_email="luis.natera@lac.mx",
)
