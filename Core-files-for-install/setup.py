from distutils.core import setup
import distutils.sysconfig


setup(name='PerformingData',
      version='0.1',
      description='Python Distribution Utilities',
      author='PerformingData',
      author_email='mrl@nottingham.ac.uk',
      url='http://www.performingdata.wp.horizon.ac.uk',
      download_url = 'https://github.com/paultennent/performingdata/tarball/0.1',
      packages = ['performingdata'],
      data_files = [(distutils.sysconfig.get_python_lib() + '/performingdata', ['performingdata/fastdatastore.exe', 'performingdata/pthreadGC2.dll', 'performingdata/sqlite3.dll'])]
      )



     
