lint:
	flake8 .
	mypy .

unittest:
	python -m unittest discover -s test -p "*.py"