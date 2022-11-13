from setuptools import setup, find_packages

setup(
    name="kz-api-elasticsearch",
    version="0.0.2",
    author="zer0k-z",
    packages=find_packages(),
    license="MIT",
    description="Continuously upload kz records to an elastic node",
    entry_points ={
            'console_scripts': [
                'kzcontinue = kzcontinue.kzcontinue:main'
            ]
    },
    install_requires=["elasticsearch", "requests"],
    zip_safe=False,
)