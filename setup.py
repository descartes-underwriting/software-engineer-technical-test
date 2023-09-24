import setuptools
from pathlib import Path

SRC_DIR = "src"

package_name = "earthquakes"


def get_long_description(filename="README.md"):
    with open(filename, "r") as fh:
        long_description = fh.read()
    return long_description


def extract_description(long_description):
    for section in long_description.split("## "):
        if section.startswith("Description"):
            return section.split("\n\n")[1]


def get_install_requires():
    requirement_paths = get_requirement_paths()
    return filter_libraries(requirement_paths)


def get_requirement_paths(
    requirement_dir: Path = Path("requirements"),
    included_names: list = ["prod"],
):
    requirement_paths = []
    for file_path in requirement_dir.glob("**/*.txt"):
        if file_path.with_suffix("").name in included_names:
            requirement_paths.append(file_path)
        if file_path.parent.name in included_names:
            requirement_paths.append(file_path)
    return requirement_paths


def filter_libraries(
    requirement_paths,
    operators=["=", "<", ">"],
) -> list:
    install_requires = []
    for requirement_path in requirement_paths:
        libs = []
        with open(requirement_path) as fh:
            for lib in fh.read().split("\n"):
                if any((operator in lib) for operator in operators):
                    libs.append(lib)
        install_requires.extend(libs)
    return install_requires


def get_data_files(src_dir=SRC_DIR):
    data_files = []
    for filepath in Path(src_dir).glob("**/*.json"):
        data_files.append(str(filepath))
    return data_files


def get_python_version():
    return "3.8"


long_description = get_long_description()
description = extract_description(long_description)
install_requires = get_install_requires()
data_files = get_data_files()
python_version = get_python_version()


setuptools.setup(
    name=package_name,
    version="1.0.dev.0",  # to prevent errors in newer python versions
    author="Descartes Underwriting SAS",
    author_email="alexandre.cameron@descartesunderwriting.com",
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="",
    packages=setuptools.find_packages(where=SRC_DIR),
    package_dir={"": SRC_DIR},
    classifiers=[
        "Programming Language :: Python :: 3",
        "(c) Descartes Underwriting SAS",
        "Operating System :: OS Independent",
    ],
    python_requires=f">={python_version}",
    install_requires=install_requires,
    data_files=data_files,
)
