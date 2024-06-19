
# Locust tests README

## Overview
This README explains how to run Locust tests for `find-moj-data` and `datahub`

## Prerequisites
- Python 3.6+
- Locust (install using `pip install locust`)

## Project Structure
```
your_project_directory/
│
├── locustfile_datahub.py
├── locustfile_find_moj_data.py
├── README.md
└── requirements.txt
```

## Installation
1. **Clone the repository** (if you haven't already):
    ```sh
    git clone https://github.com/ministryofjustice/data-catalogue
    cd data-catalogue
    ```

2. **Install the dependencies**:
    ```sh
    pip install -r requirements.txt
    ```

3. **Export datahub auth token if testing datahub**
   ```sh
      export DATAHUB_TEST_AUTH_TOKEN="your-datahub-token"
   ```

4. **Export find-moj-data session-id if testing app**
 * Open the app on your browser and login 
 * Open the search, and using the developer tools on Chrome, open the network tab.
 * Find the Requests Header, then the cookies, and inside the `Cookie` header there is a `sessionid` that needs to be exported.

   ```shell
      export SESSION_ID="your session id"
   ```
## Running Locust

### Running datahub tests
To run Locust with `locustfile_datahub.py`:
```sh
locust -f locustfile_datahub.py
```

### Running find-moj-data tests
To run Locust with `locustfile_find_moj_data.py`:
```sh
locust -f locustfile_find_moj_data.py
```

## Configurations
You can customize the behavior of Locust using various command-line options. Here are some useful options:

- `-u`, `--users` - Number of concurrent users (total users to simulate).
- `-r`, `--spawn-rate` - Rate to spawn users (users per second).
- `-H`, `--host` - Host to load test.

### Example
To simulate 100 users with a spawn rate of 10 users per second, targeting the host `http://example.com`, using `file1.py`:
```sh
locust -f locustfile_datahub.py --users 100 --spawn-rate 10 --host http://example.com
```

## Accessing the Web Interface
After running Locust, open your web browser and go to:
```
http://localhost:8089
```
Here, you can start the test, view statistics, and monitor the progress.

## Customizing Locustfiles
Each Locustfile should define user behaviors by creating tasks. Here is a simple example of what a Locustfile might look like:

```python
from locust import HttpUser, TaskSet, task, between

class UserBehavior(TaskSet):
    
    @task(1)
    def index(self):
        self.client.get("/")

    @task(2)
    def about(self):
        self.client.get("/about")

class WebsiteUser(HttpUser):
    tasks = [UserBehavior]
    wait_time = between(1, 5)
```

## Additional Resources
- [Locust Documentation](https://docs.locust.io/en/stable/)
- [Locust GitHub Repository](https://github.com/locustio/locust)

## Contributing
Contributions are welcome! Please submit a pull request or open an issue to discuss what you would like to change.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

Feel free to customize this README further based on your specific project needs.
