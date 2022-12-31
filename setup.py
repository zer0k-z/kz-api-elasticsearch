from setuptools import setup, find_packages

setup(
    name="kz-api-elasticsearch",
    version="0.0.5",
    author="zer0k-z",
    packages=find_packages(),
    license="MIT",
    description="Continuously upload kz records to an elastic node",
    entry_points ={
            'console_scripts': [
                'kzcontinue = kzcontinue.kzcontinue:main',
                'elasticsteam = elasticsteam.elasticsteam:main'
            ]
    },
    install_requires=["elasticsearch==7.17.7", "requests", "steam"],
    zip_safe=False,
)