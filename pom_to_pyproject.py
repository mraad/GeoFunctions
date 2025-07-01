import toml
import xml.etree.ElementTree as ET


def get_maven_version(pom_file="pom.xml"):
    tree = ET.parse(pom_file)
    root = tree.getroot()
    ns = {'mvn': 'http://maven.apache.org/POM/4.0.0'}
    version = root.find('mvn:version', ns)
    if version is None:
        version = root.find('version')
    return version.text.strip() if version is not None else None


def set_pyproject_version(version, pyproject_file="pyproject.toml"):
    with open(pyproject_file, "r") as f:
        data = toml.load(f)
    data["project"]["version"] = version
    with open(pyproject_file, "w") as f:
        toml.dump(data, f)


if __name__ == "__main__":
    version = get_maven_version("pom.xml")
    if not version:
        raise ValueError("Could not find version in pom.xml")
    set_pyproject_version(version, "pyproject.toml")
    print(f"Updated pyproject.toml to version: {version}")
