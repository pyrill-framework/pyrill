import os
from pathlib import Path

from setuptools import find_packages, setup

import setup_utils

PACKAGE_DIR = os.environ['PACKAGE_DIR']
PACKAGE_NAME = os.environ['PACKAGE_NAME']
PACKAGE_DESCRIPTION = os.environ['PACKAGE_DESCRIPTION']
PACKAGE_REPOSITORY = os.environ['PACKAGE_REPOSITORY']
PACKAGE_AUTHOR_EMAIL = os.environ['PACKAGE_AUTHOR_EMAIL']
PACKAGE_AUTHOR = os.environ['PACKAGE_AUTHOR']

version_file = Path(__file__).parent / PACKAGE_DIR / '__version__.py'

version = setup_utils.get_version_from_file(version_file)

if os.environ.get('PACKAGE_DEVELOPMENT') is not None:
    version = setup_utils.get_development_version_from_file(version_file,
                                                            int(os.environ.get('PACKAGE_DEVELOPMENT')),
                                                            os.environ.get('PACKAGE_COMMIT'))

    setup_utils.set_version_to_file(version_file, version)

requirements = setup_utils.get_requirements_from_file(Path(__file__).parent / 'requirements.txt')

setup(
    name=PACKAGE_NAME,
    url=PACKAGE_REPOSITORY,
    author=PACKAGE_AUTHOR,
    version=version,
    author_email=PACKAGE_AUTHOR_EMAIL,
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Development Status :: 4 - Beta'],
    packages=find_packages(include=[f'{PACKAGE_DIR}*']),
    install_requires=requirements,
    extras_require={":python_version<'3.8'": ["typing-extensions"],
                    ":python_version<'3.7'": ["dataclasses", "async_exit_stack"]},
    description=PACKAGE_DESCRIPTION,
    long_description=(Path(__file__).parent / 'README.rst').read_text(),
    long_description_content_type='text/x-rst',
    zip_safe=True
)
