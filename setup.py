'''
Our setup file. To install the package on your machine, run:

sudo python setup.py install
'''

from setuptools import setup

setup(name='parallelogram',
      version='0.1',
      description='A parallelization library for distributed machines',
      url='https://github.com/noahyonack/parallelogram',
      author='Aaron Kabcenell, Jack Murtagh, Noah Yonack',
      author_email='noahyonack@college.harvard.edu',
      license='APACHE 2.0',
      packages=['parallelogram'],
      # this parameter holds a list of 3rd-party 
      # packages that our module depends on
      install_requires= ['cloudpickle', 'psutil'],
      test_suite='nose.collector',
    	tests_require=['nose'],
      zip_safe=False)