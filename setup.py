from setuptools import setup, find_packages

setup(
    name='folderdb',
    version='1.0.0',
    packages=['db'],
    package_dir={'db': 'db'},
    description='A directory-based database package',
    author='OmgRod',
    license='MIT',
    install_requires=[
        'cryptography'
    ],
)
