try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


setup(
    name='tuplespace',
    url='http://github.com/alecthomas/tuplespace',
    download_url='http://github.com/alecthomas/tuplespace',
    version='0.1.0',
    description='Python client for a Tuple Space service.',
    license='BSD',
    platforms=['any'],
    author='Alec Thomas',
    author_email='alec@swapoff.org',
    py_modules=['tuplespace'],
    )
