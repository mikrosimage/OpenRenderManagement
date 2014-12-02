from setuptools import setup

setup(
    name='OpenRenderManagement',
    version='1.7.7',
    description="Open source render farm dispatcher",
    long_description="",
    author='acs',
    author_email='opensource@mikrosimage.eu',
    license='MIT',
    packages=['octopus', 'puliclient', 'pulitools'],
    zip_safe=False,
    install_requires=[
        'tornado',
        'sqlobject',
        'requests',
    ],
)
