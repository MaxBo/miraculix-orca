# -*- coding: utf-8 -*-
#
from setuptools import setup, find_packages


setup(
    name="extractiontools",
    version="0.1",
    description="tools to extract gis data and produce networks etc",
    classifiers=[
        "Programming Language :: Python",
        "Environment :: Plugins",
        "Intended Audience :: System Administrators",
        "License :: Other/Proprietary License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
    ],
    packages=find_packages('src', exclude=['ez_setup']),
    namespace_packages=['extractiontools'],

    package_dir={'': 'src'},
    include_package_data=True,
    zip_safe=False,
    data_files=[],

    extras_require=dict(
        extra=[],
        docs=[
            'z3c.recipe.sphinxdoc',
            'sphinxcontrib-requirements'
        ],
        test=[]
    ),

    install_requires=[
        'setuptools',
        'psycopg2>=2.4.6',
        'numpy>=1.9',
        'sqlparse',
        # -*- Extra requirements: -*-
    ],

)
