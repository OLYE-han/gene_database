from setuptools import Extension, setup

import numpy


setup(
    ext_modules=[
        Extension(
            "pyramid_rabitq._native",
            ["src/pyramid_rabitq/native_scan.cpp"],
            include_dirs=[numpy.get_include()],
            language="c++",
            extra_compile_args=["-O3", "-std=c++17"],
        )
    ]
)

