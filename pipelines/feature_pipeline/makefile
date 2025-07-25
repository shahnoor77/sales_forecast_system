run-fetch:
	poetry run python src/fetch_data.py
	
	
run-explore:
	poetry run python src/explore.py
	
run-clean:
	poetry run python src/data_cleaning.py

run-analyze:
	poetry run python src/analyze_data.py

run-combine:
	poetry run python src/combining_tables.py

run-transform:
	poetry run python src/transform_data.py	

lint:
	poetry run ruff check --fix

format:
	poetry run ruff format .

lint-and-format: lint format