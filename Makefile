.PHONY: tests

.DEFAULT:
	cd tests/docker/ && $(MAKE) $(MAKECMDGOALS)

tests:
	@echo "run tests"
