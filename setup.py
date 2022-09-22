from pathlib import Path

from setuptools import find_packages, setup

readme_file = Path(__file__).parent / 'README.md'
if readme_file.exists():
    with readme_file.open() as f:
        long_description = f.read()
else:
    # When this is first installed in development Docker, README.md is not available
    long_description = ''

setup(
    name='multinet',
    version='0.1.0',
    description='',
    long_description=long_description,
    long_description_content_type='text/markdown',
    license='Apache 2.0',
    author='Kitware, Inc.',
    author_email='kitware@kitware.com',
    keywords='',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Web Environment',
        'Framework :: Django :: 3.0',
        'Framework :: Django',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python',
    ],
    python_requires='>=3.8',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'celery',
        'django~=3.2.11',
        'django-admin-display',
        'django-allauth',
        'django-composed-configuration[dev,prod]>=0.10.0',
        'django-configurations[database,email]',
        'django-extensions',
        'django-filter',
        'django-guardian',
        'django-oauth-toolkit==1.3.2',
        'django-s3-file-field[minio,boto3]>=0.3.0',
        # DRF 3.14 is incompatible with drf-yasg, see
        # https://github.com/axnsan12/drf-yasg/issues/810
        'djangorestframework<3.14',
        'drf-extensions',
        'drf-yasg',
        'more-itertools',
        'python-arango',
        # Production-only
        'gunicorn',
        # Development-only
        'django-debug-toolbar',
    ],
    extras_require={'dev': ['ipython', 'tox']},
)
