VERSION=`python3 -c "import pymamba; print(pymamba.__version__)"`

all:
	@echo "Current version is: $(VERSION)"

test:
	pytest --cov=pymamba --cov-report=term-missing 

clean:
	@rm -rfv unit-db
	@rm -rfv examples/perfDB

pypitest:
	@rm -f dist/*
	@python3 setup.py sdist
	@python3 setup.py bdist_wheel
	@twine register -r pypitest dist/pymamba-$(VERSION)-py3-none-any.whl
	@twine upload -r pypitest dist/pymamba-$(VERSION).tar.gz

pypirelease:
	@twine register -r pypi dist/pymamba-$(VERSION)-py3-none-any.whl
	@twine upload -r pypi dist/pymamba-$(VERSION).tar.gz
