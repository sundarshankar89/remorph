all: clean dev fmt lint test

clean:
	rm -fr .venv clean htmlcov .mypy_cache .pytest_cache .ruff_cache .coverage coverage.xml

dev:
	pip3 install hatch
	hatch env create
	hatch run pip install -e '.[test]'
	hatch run which python

lint:
	hatch run verify

fmt: fmt-python fmt-scala

fmt-python:
	hatch run fmt

fmt-scala:
	mvn validate -Pformat

test: test-python test-scala

setup_spark_remote:
	.github/scripts/setup_spark_remote.sh

test-python: setup_spark_remote
	hatch run test

test-scala:
	mvn test -f pom.xml

integration:
	hatch run integration

coverage:
	hatch run coverage && open htmlcov/index.html

build_core_jar:
	mvn --file pom.xml -pl core package

clean_coverage_dir:
	rm -fr ${OUTPUT_DIR}

python_coverage_report:
	hatch run python src/databricks/labs/remorph/coverage/remorph_snow_transpilation_coverage.py
	hatch run pip install --upgrade sqlglot
	hatch -e sqlglot-latest run python src/databricks/labs/remorph/coverage/sqlglot_snow_transpilation_coverage.py
	hatch -e sqlglot-latest run python src/databricks/labs/remorph/coverage/sqlglot_tsql_transpilation_coverage.py

antlr_coverage_report: build_core_jar
	java -jar $(wildcard core/target/remorph-core-*-SNAPSHOT.jar) '{"command": "debug-coverage", "flags":{"src": "$(abspath ${INPUT_DIR_PARENT}/snowflake)", "dst":"$(abspath ${OUTPUT_DIR})", "source-dialect": "Snow", "extractor": "full"}}'
	java -jar $(wildcard core/target/remorph-core-*-SNAPSHOT.jar) '{"command": "debug-coverage", "flags":{"src": "$(abspath ${INPUT_DIR_PARENT}/tsql)", "dst":"$(abspath ${OUTPUT_DIR})", "source-dialect": "Tsql", "extractor": "full"}}'

dialect_coverage_report: clean_coverage_dir antlr_coverage_report python_coverage_report
	hatch run python src/databricks/labs/remorph/coverage/local_report.py

antlr-coverage: build_core_jar
	echo "Running coverage for snowflake"
	java -jar $(wildcard core/target/remorph-core-*-SNAPSHOT.jar) '{"command": "coverage", "flags":{"src": "$(abspath ${INPUT_DIR_PARENT}/snowflake)", "dst":"$(abspath ${OUTPUT_DIR})", "source-dialect": "Snow", "extractor": "full"}}'
	echo "Running coverage for tsql"
	java -jar $(wildcard core/target/remorph-core-*-SNAPSHOT.jar) '{"command": "coverage", "flags":{"src": "$(abspath ${INPUT_DIR_PARENT}/tsql)", "dst":"$(abspath ${OUTPUT_DIR})", "source-dialect": "Tsql", "extractor": "full"}}'
	OUTPUT_DIR=.venv/antlr-coverage hatch run python src/databricks/labs/remorph/coverage/local_report.py

antlr-lint:
	mvn compile -DskipTests exec:java -pl linter --file pom.xml -Dexec.args="-i core/src/main/antlr4 -o .venv/linter/grammar -c true"

dev-cli:
	mvn -f core/pom.xml dependency:build-classpath -Dmdep.outputFile=target/classpath.txt

estimate-coverage: build_core_jar
	java -jar $(wildcard core/target/remorph-core-*-SNAPSHOT.jar) '{"command": "debug-estimate", "flags":{"dst":"$(abspath ${OUTPUT_DIR})", "source-dialect": "snowflake", "console-output": "true"}}'
