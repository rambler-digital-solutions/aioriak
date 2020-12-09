REPO ?= peopleai
PIP_CFG ?= ~/.config/pip/pip.conf
BUMP_LEVEL ?= patch
ARTIFACTORY_REPO ?= https://peopleai.jfrog.io/peopleai/api/pypi/pai-pypi

.PHONY: bumpversion
bumpversion:
	@bumpversion $(BUMP_LEVEL)

.PHONY: clean
clean:
	@python setup.py clean --all

.PHONY: build
build: clean
	@python setup.py sdist bdist_wheel

.PHONY: publish
publish:
	@twine upload -r $(REPO) --config-file $(PIP_CFG) dist/*

.PHONY: publish-circle
publish-circle:
	@twine upload --verbose --repository-url $(ARTIFACTORY_REPO) -u ${ARTIFACTORY_PYPI_USERNAME} -p ${ARTIFACTORY_PYPI_PASSWORD} dist/*
