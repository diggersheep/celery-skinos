from setuptools import find_packages
from setuptools import setup

from skinos import VERSION

with open('README.md') as f:
    long_description = f.read()

NAME = "celery-skinos"

def recursive_requirements(requirement_file, libs, links, path=''):
    if not requirement_file.startswith(path):
        requirement_file = os.path.join(path, requirement_file)
    with open(requirement_file) as requirements:
        for requirement in requirements.readlines():
            if requirement.startswith('-r'):
                requirement_file = requirement.split()[1]
                if not path:
                    path = requirement_file.rsplit('/', 1)[0]
                recursive_requirements(requirement_file, libs, links,
                                       path=path)
            elif requirement.startswith('-f'):
                links.append(requirement.split()[1])
            elif requirement.startswith('-e'):
                links.append(requirement.split()[1])
            else:
                libs.append(requirement)

libraries, dependency_links = [], []
recursive_requirements('requirements.txt', libraries, dependency_links)

setup(
    name=NAME,
    version=VERSION,
    packages=find_packages(),
    install_requires=libraries,
    dependency_links=dependency_links,
    long_description=long_description,
    description='',
    author='di-dip-unistra',
    author_email='di-dip@unistra.fr',
    maintainer='di-dip-unistra',
    maintainer_email='di-dip@unistra.fr',
    url='',
    download_url='',
    license='PSF',
    keywords=['celery', 'AMQP', 'unistra', 'Université de Strasbourg'],
    include_package_data=True,
)
