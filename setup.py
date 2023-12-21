import setuptools

setuptools.setup(
    name="dataflow-pubsub-dlt",
    version="0.1.0",
    install_requires=["google-cloud-pubsub", "dlt[postgres,gs]"],
    packages=setuptools.find_packages(),
)
