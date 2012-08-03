from distutils.core import setup
import os


def is_package(path):
    return (os.path.isdir(path) and
            os.path.isfile(os.path.join(path, '__init__.py')))


def find_packages(path, base=''):
    packages = {}
    for item in os.listdir(path):
        dir = os.path.join(path, item)
        if is_package(dir):
            if base:
                module_name = "%(base)s.%(item)s" % vars()
            else:
                module_name = item
            packages[module_name] = dir
            packages.update(find_packages(dir, module_name))
    return packages

SCRIPTS = [
    "scripts/dispatcherd.py",
    "scripts/workerd.py"
]
PACKAGE_DIR = find_packages('src')
PACKAGES = PACKAGE_DIR.keys()
DATA_FILES = [
    ("etc/puli", ["etc/puli/workers.lst"]),
    ("etc/puli/pools", []),
]

if __name__ == '__main__':
    setup(
        name='OpenDispatcher',
        version='1.0',
        description='OpenDispatcher aka Puli',
        license='Modified BSD',
        author='Mikros Image',
        author_email='dev@mikrosimage.eu',
        url='http://github.com/mikrosimage/puli',
        package_dir=PACKAGE_DIR,
        packages=PACKAGES,
        scripts=SCRIPTS,
        data_files=DATA_FILES,
    )
