from lumy_middleware import __version__
import setuptools
import os.path

REPO_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))


description = 'Lumy Middleware'
homepage = 'https://github.com/DHARPA-Project/lumy-middleware'
author = 'DHARPA'
license = 'AGPL-3.0-only'

with open('README.md', 'r') as fh:
    long_description = fh.read()

setup_args = dict(
    name='lumy-middleware',
    version=__version__,
    url=homepage,
    author=author,
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    install_requires=[
        'ipython',
        'ipykernel',
        'tinypubsub>=0.1.0',
        'stringcase>=1.2.0',
        'dataclasses-json>=0.5.2',
        'pyyaml',
        'kiara[all]==0.0.4',
        'kiara_modules.core==0.0.3',
        'kiara_modules.network_analysis==0.0.2',
        'pandas>=1.2.4',
        'appdirs>=1.4.4',
        'stevedore>=3.3.0',
        # will be installed dynamically later
        'lumy-modules.network-analysis==0.1.0'
    ],
    zip_safe=False,
    include_package_data=True,
    python_requires=">=3.7",
    license=license,
    platforms="Linux, Mac OS X, Windows",
    keywords=["Jupyter", "JupyterLab", "JupyterLab3"],
    classifiers=[
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Framework :: Jupyter",
    ],
    entry_points={
        'kiara.modules': [
            'lumydev = lumy_middleware.dev:modules'
        ],
        'kiara.pipelines': [
            'lumydev = lumy_middleware.dev:pipelines'
        ]
    }
)

if __name__ == "__main__":
    setuptools.setup(**setup_args)
