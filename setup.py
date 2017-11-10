from setuptools import setup, find_packages

setup(
    name='launch_day',
    version='0.1',
    packages=['launch_day'],
    url='',
    license='',
    author='tspurway',
    author_email='tspurway@mozilla.com',
    description='Lambda for getting launch day dashboard data',
    install_requires=['psycopg2', 'boto'],
    scripts=['launch-day.py'],
    zip_safe=False,
)
