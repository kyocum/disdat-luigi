#!/usr/bin/env bash

echo "Building Disdat package for local installation or PyPi . . ."

# Using svc versioning.  Builds on what you set the tag to be.
# so: git commit -am "<version>"
# and then: git tag <version>

# Remove the prior tar ball from the context.template
rm -rf  disdatluigi/infrastructure/dockerizer/context.template/disdat_luigi-*.tar.gz
rm -rf  dist/disdat_luigi-*.tar.gz

# Create a new sdist
python setup.py sdist

# Copy over to the context.template.
cp dist/disdat_luigi-*.tar.gz disdatluigi/infrastructure/dockerizer/context.template/.

# Create a new sdist that will have that tar.gz in the template
python setup.py sdist

# publish to test pypi
if false; then
    echo "Uploading to PYPI test and real"
    #twine upload --repository-url https://test.pypi.org/legacy/ dist/disdat-*.tar.gz
    # Test: pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple disdat
    # now do it for real
    twine upload dist/disdat_luigi-*.tar.gz
fi

echo "Finished"

