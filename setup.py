from setuptools import setup

setup(name='neo_my2pg',
      version='0.9',
      description='Migration package from mysql to postgresql',
      url='https://github.com/the4thdoctor/neo_my2pg',
      author='Federico Campoli',
      author_email='4thdoctor.gallifrey@gmail.com',
      license='GNU General Public License v3 (GPLv3)',
      packages=['neo_my2pg'],
      zip_safe=False,
      install_requires=[
          'SQLAlchemy ==0.7.9',
      ],)