# Real-Time Log Alerting System

The application processes logs from CSV files to detect and alert specific error patterns

## Arhitecture 

The application separates Rules and proccesing logic

File src/rules.py contains Rule interface as well as implementations of specific rulles. 

Processing of CSV files happens in batches to increase speed and make processing of huge files possible


## How to Run

The application is containerized using Docker for a consistent and easy setup.

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/raijinstorm/task9_alert_project.git
    cd task9_alert_project
    ```

2.  **Add your data:**
    Place your input CSV file (e.g., `sorted.csv`) inside the `/data` directory.

3.  **Build and run the application:**
    Use Docker Compose to build the image and run the container with a single command.
    ```bash
    docker-compose up --build
    ```

## How to Add a New Rule

Adding a new alerting rule is a simple 3-step process:

1.  **Define the Rule Class:**
    Open `src/rules.py` and create a new class that inherits from the base `Rule` class. You must implement the  `check` method with your custom logic.

    ```python
    class MyNewRule(Rule):

        def check(self, log_entry):
            # Your custom logic here
            pass
    ```
2. **Activate the Rule:** 
```python
    # in src/main.py
    from src.rules import MyNewRule # <-- Import new rule
```

Finally, just run `docker-compose up --build` to run the application with your new rule included.