#!/usr/bin/env python
# coding: utf-8

# # <span style="color:blue">PART 2: REST API INTEGRATION</span>
# 

# ---
# ## 2.1 Understanding APIs: The Restaurant Analogy
# 
# Imagine a **restaurant**:
# 
# | Concept | API Equivalent |
# |---|---|
# | 🍳 Kitchen | Server with data |
# | 📋 Menu | API documentation |
# | 🧑‍🍳 Waiter | API endpoints |
# | 📝 Your order | HTTP request |
# | 🍽️ Your food | HTTP response |
# 
# > You **don't** go into the kitchen (database). You tell the waiter what you want, and they bring it to you.
# 
# ---
# 
# ### <span style="color:blue">HTTP Methods = Actions</span>
# 
# | Method | Meaning | Description |
# |---|---|---|
# | **`GET`** | "Show me the menu" | Retrieve data |
# | **`POST`** | "I want to order this" | Create new data |
# | **`PUT`** | "Change my entire order" | Replace data |
# | **`PATCH`** | "Add extra cheese" | Update part of data |
# | **`DELETE`** | "Cancel my order" | Remove data |
# 
# ---
# 
# ### <span style="color:blue">Status Codes = Kitchen's Response</span>
# 
# | Code | Message | Meaning |
# |---|---|---|
# | **200** | OK | "Here's your food!" |
# | **201** | Created | "Order placed successfully!" |
# | <span style="color:red">**400**</span> | Bad Request | "That's not on the menu" |
# | <span style="color:red">**401**</span> | Unauthorized | "You need to pay first" |
# | <span style="color:red">**404**</span> | Not Found | "We don't have that dish" |
# | <span style="color:red">**429**</span> | Too Many Requests | "You're ordering too fast!" |
# | <span style="color:red">**500**</span> | Server Error | "Kitchen is on fire" |

# ---
# ## 2.2 Real Example: GitHub API
# 
# **GitHub** provides free APIs to access public repository data. Let's explore!
# 
# > 📌 We use the `requests` library to make HTTP calls and `pandas` to work with the data.

# In[1]:


import requests
import pandas as pd
import json
from datetime import datetime


# Example 1: Get repository information
def get_repo_info(owner, repo):
    """
    Fetch information about a GitHub repository.

    Args:
        owner: Repository owner (e.g., 'pandas-dev')
        repo: Repository name (e.g., 'pandas')

    Returns:
        dict: Repository information
    """
    # API endpoint - GitHub's REST API base URL + path to specific repo
    url = f"https://api.github.com/repos/{owner}/{repo}"

    # Make GET request to the API endpoint
    response = requests.get(url)

    # Check status code to determine if request succeeded
    print(f"Status Code: {response.status_code}")
    print(f"Response Headers:")
    for key, value in list(response.headers.items())[:5]:
        print(f"  {key}: {value}")

    # Parse JSON response only if request was successful
    if response.status_code == 200:
        data = response.json()

        # Extract only the relevant fields we care about
        repo_info = {
            "name": data["name"],
            "full_name": data["full_name"],
            "description": data["description"],
            "stars": data["stargazers_count"],
            "forks": data["forks_count"],
            "watchers": data["watchers_count"],
            "open_issues": data["open_issues_count"],
            "language": data["language"],
            "created_at": data["created_at"],
            "updated_at": data["updated_at"],
            "size": data["size"],
            "license": data["license"]["name"] if data["license"] else "No license",
        }
        return repo_info
    else:
        print(f"Error: {response.status_code}")
        return None


# Try it!
repo_info = get_repo_info("pandas-dev", "pandas")

if repo_info:
    print("\n=== Repository Information ===")
    for key, value in repo_info.items():
        print(f"{key}: {value}")


# ### Expected Output:
# ```
# Status Code: 200
# Response Headers:
#   Content-Type: application/json
#   X-RateLimit-Limit: 60
#   X-RateLimit-Remaining: 59
#   ...
# 
# === Repository Information ===
# name: pandas
# full_name: pandas-dev/pandas
# description: Flexible and powerful data analysis / manipulation library for Python
# stars: 43256
# forks: 17843
# ...
# ```
# 
# ---
# 
# ### <span style="color:blue">Understanding the Response</span>
# 
# An **HTTP Response** has 3 parts:
# 
# ```
# 1. Status Line:   HTTP/1.1 200 OK
# 
# 2. Headers:
#    Content-Type: application/json
#    X-RateLimit-Limit: 60        <-- Max requests per hour
#    X-RateLimit-Remaining: 59    <-- Requests left
# 
# 3. Body (JSON):
#    {
#      "name": "pandas",
#      "stars": 43256,
#      ...
#    }
# ```

# ---
# ## 2.3 Handling Authentication
# 
# Many APIs require **authentication** to:
# 1. Track usage
# 2. Prevent abuse
# 3. Provide personalized data
# 
# ---
# 
# ### <span style="color:blue">Types of Authentication</span>
# 
# #### **Type 1: API Key in Header** *(Most Common)*

# In[2]:


import os
from dotenv import load_dotenv  # pip install python-dotenv

# Load API key from .env file (keeps secrets out of your code)
load_dotenv()


# In[3]:


api_key = os.getenv("GITHUB_TOKEN")

# Add the token to request headers for authentication
headers = {
    "Authorization": f"Bearer {api_key}",  # Token-based auth
    "Accept": "application/vnd.github.v3+json",  # Tell server what format we want
    "User-Agent": "Library-Tutorial-App",  # Identify your app
}

url = "https://api.github.com/repos/pandas-dev/pandas"
response = requests.get(url, headers=headers)
print(f"Status Code: {response.status_code}")
if response.status_code == 200:
    data = response.json()
    print(f"Repository: {data['full_name']}")
    print(f"Stars: {data['stargazers_count']}")
else:
    print(f"Error: {response.status_code}")


# #### Create a `.env` file in your project directory:
# 
# ```
# GITHUB_TOKEN=ghp_your_token_here
# OPENWEATHER_KEY=your_key_here
# ```
# 
# #### <span style="color:red">⚠️ Why `.env`?</span>
# - **Never** hardcode secrets in code!
# - Different keys for dev/production
# - Keeps secrets out of version control
# 
# #### Add to `.gitignore`:
# ```
# .env
# *.env
# ```
# 
# ---
# 
# #### **Type 2: API Key in Query Parameters**

# In[4]:


# Example: OpenWeather API uses key as a query parameter
api_key = os.getenv('OPENWEATHER_KEY')
print(f"Using OpenWeather API Key: {api_key[:4]}...")  # Print only first 4 chars for security


# In[5]:


# Parameters are automatically appended to the URL as ?q=Cairo&appid=...&units=metric
params = {
    "q": "Cairo",
    "appid": api_key,
    "units": "metric",  # Celsius instead of Kelvin
}

response = requests.get(
    "https://api.openweathermap.org/data/2.5/weather", params=params
)
print(f"Status Code: {response.status_code}")
if response.status_code == 200:
    data = response.json()
    print(f"City: {data['name']}")
    print(f"Temperature: {data['main']['temp']} °C")
    print(f"Weather: {data['weather'][0]['description']}")
else:
    print(f"Error: {response.status_code}")


# #### **Type 3: OAuth** *(Complex but Secure)*

# In[ ]:


# OAuth flow (simplified):
# 1. User authorizes your app
# 2. You get an access token
# 3. Use token for requests
# This is beyond our scope but good to know!


# ---
# ## <span style="color:blue">2.4 Advanced: Pagination</span>
# 
# **Problem**: API returns 100 results, but there are **10,000!**
# 
# **Solution**: **Pagination** — fetch data in multiple pages, just like a book.
# 
# ### <span style="color:blue">Pagination Strategies</span>
# 
# **1. Page-based (GitHub):**
# ```
# /repos?page=1&per_page=100
# /repos?page=2&per_page=100
# ```
# 
# **2. Offset-based:**
# ```
# /repos?offset=0&limit=100
# /repos?offset=100&limit=100
# ```
# 
# **3. Cursor-based** *(most efficient)*:
# ```
# /repos?cursor=abc123
# /repos?cursor=def456
# 
# Response includes next cursor:
# {
#   "data": [...],
#   "next_cursor": "def456"
# }
# ```

# In[6]:


import time


def get_all_repos(org_name, max_pages=None):
    """
    Fetch all repositories for an organization using pagination.
    GitHub API returns 30 repos per page by default.
    """
    all_repos = []
    page = 1

    while True:
        # Check if we've reached max_pages (useful for testing/limiting)
        if max_pages and page > max_pages:
            break

        print(f"Fetching page {page}...")

        # Add page parameter to tell the API which chunk of results we want
        params = {"page": page, "per_page": 100}  # Max allowed by GitHub

        url = f"https://api.github.com/orgs/{org_name}/repos"
        response = requests.get(url, params=params)

        if response.status_code != 200:
            print(f"Error: {response.status_code}")
            break

        data = response.json()

        # If no data returned, we've gone past the last page
        if not data or len(data) == 0:
            print("No more results!")
            break

        all_repos.extend(data)
        page += 1

        # Be polite - wait between requests to avoid rate limiting
        time.sleep(0.5)

    return all_repos


# Example: Get all pandas-dev repos (limit to 3 pages for demo)
repos = get_all_repos("pandas-dev", max_pages=3)
print(f"\nFetched {len(repos)} repositories")

# Convert list of dicts to a clean DataFrame
df = pd.DataFrame(
    [
        {
            "name": repo["name"],
            "stars": repo["stargazers_count"],
            "language": repo["language"],
            "description": repo["description"],
        }
        for repo in repos
    ]
)

print("\nTop 10 by Stars:")
print(df.sort_values("stars", ascending=False).head(10))


# ---
# ## 2.5 Rate Limiting & Retry Logic
# 
# <span style="color:red">**Problem**</span>: APIs limit requests to prevent abuse.
# 
# ### GitHub Rate Limits:
# - **Unauthenticated**: `60` requests/hour
# - **Authenticated**: `5,000` requests/hour
# 
# ---
# 
# ### <span style="color:blue">Custom Rate Limiter Class</span>

# In[ ]:


import time
from datetime import datetime


class RateLimiter:
    """
    Smart rate limiter that tracks API usage.
    Uses a sliding time window to count recent requests.
    """

    def __init__(self, max_requests=60, time_window=3600):
        """
        Args:
            max_requests: Maximum requests allowed in the time window
            time_window: Time window in seconds (3600 = 1 hour)
        """
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []  # List of timestamps of past requests

    def wait_if_needed(self):
        """Wait if we've hit the rate limit before making a new request."""
        now = time.time()

        # Remove old timestamps outside the sliding time window
        self.requests = [
            req_time for req_time in self.requests if now - req_time < self.time_window
        ]

        # If we've used up our quota, sleep until the oldest request expires
        if len(self.requests) >= self.max_requests:
            oldest_request = self.requests[0]
            sleep_time = self.time_window - (now - oldest_request)
            if sleep_time > 0:
                print(
                    f"⏰ Rate limit reached. Sleeping for {sleep_time:.1f} seconds..."
                )
                time.sleep(sleep_time)
            self.requests = []  # Clear after sleeping

        # Record the timestamp of this new request
        self.requests.append(now)


# Usage: 10 requests per minute limit
limiter = RateLimiter(max_requests=10, time_window=60)

url = "https://api.github.com/repos/pandas-dev/pandas"
for i in range(15):
    limiter.wait_if_needed()  # Automatically pauses if limit is hit
    response = requests.get(url)
    print(f"Request {i+1} completed")


# ### <span style="color:blue">Checking API Limits from Headers</span>

# In[8]:


def check_rate_limit(response):
    """
    Check rate limit info from response headers.
    GitHub includes rate limit details in every response.
    """
    if 'X-RateLimit-Limit' in response.headers:
        limit = int(response.headers['X-RateLimit-Limit'])
        remaining = int(response.headers['X-RateLimit-Remaining'])
        reset_timestamp = int(response.headers['X-RateLimit-Reset'])
        reset_time = datetime.fromtimestamp(reset_timestamp)

        print(f"Rate Limit: {remaining}/{limit}")
        print(f"Resets at: {reset_time}")

        # Warn when running low on available requests
        if remaining < 10:
            print("⚠️ Warning: Low on API requests!")

        return remaining
    return None


url = 'https://api.github.com/repos/pandas-dev/pandas'
response = requests.get(url)
check_rate_limit(response)


# ### <span style="color:blue">Automatic Retry with Exponential Backoff</span>
# 
# **What is Exponential Backoff?**
# 
# Instead of hammering the server after a failure, we **wait progressively longer** between retries:
# 
# ```
# Attempt 1: Fails → Wait  1 second
# Attempt 2: Fails → Wait  2 seconds
# Attempt 3: Fails → Wait  4 seconds
# Attempt 4: Fails → Wait  8 seconds
# Attempt 5: Success! ✅
# ```

# In[9]:


from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def create_robust_session():
    """
    Create requests session with automatic retry logic.
    The session will automatically retry failed requests using exponential backoff.
    """
    session = requests.Session()

    # Define retry strategy - what to retry, how many times, and how long to wait
    retry_strategy = Retry(
        total=5,  # Maximum number of retries
        backoff_factor=1,  # Wait 0, 1, 2, 4, 8 seconds between retries
        status_forcelist=[429, 500, 502, 503, 504],  # Retry on these status codes
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],  # Methods to retry
    )

    # Mount the retry adapter for both http and https
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


# Usage - just use this session like you would requests.get()
session = create_robust_session()
response = session.get("https://api.github.com/repos/pandas-dev/pandas")
print(f"Status: {response.status_code}")


# ---
# ## 2.6 Error Handling & Logging
# 
# **Robust code** anticipates failures and records what happened for debugging.

# In[10]:


import logging
from datetime import datetime

# Configure logging to write to both a file AND the console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('api_requests.log'),  # Saves to disk
        logging.StreamHandler(),  # Also print to console
    ],
)

logger = logging.getLogger(__name__)


def fetch_with_error_handling(url, max_retries=3):
    """
    Robust API fetch with comprehensive error handling.
    Handles: timeouts, connection errors, rate limits, server errors, bad JSON.
    """
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempt {attempt + 1}/{max_retries}: GET {url}")
            response = requests.get(url, timeout=10)  # Don't wait more than 10 seconds

            # Handle each status code type differently
            if response.status_code == 200:
                logger.info(f"✓ Success: {url}")
                return response.json()

            elif response.status_code == 404:
                logger.error(f"✗ Not Found: {url}")
                return None  # No point retrying - resource doesn't exist

            elif response.status_code == 429:
                # Server tells us how long to wait in the Retry-After header
                retry_after = int(response.headers.get('Retry-After', 60))
                logger.warning(f"Rate limited. Waiting {retry_after}s...")
                time.sleep(retry_after)
                continue

            elif response.status_code >= 500:
                # Server error - retry with exponential backoff
                logger.error(f"Server error ({response.status_code}). Retrying...")
                time.sleep(2**attempt)  # 1, 2, 4 seconds
                continue

            else:
                logger.error(f"HTTP {response.status_code}: {url}")
                response.raise_for_status()

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout on attempt {attempt + 1}")
            if attempt < max_retries - 1:
                time.sleep(2**attempt)

        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error: {e}")
            if attempt < max_retries - 1:
                time.sleep(2**attempt)

        except json.JSONDecodeError:
            logger.error("Invalid JSON response")  # Server returned non-JSON
            return None

        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None

    logger.error(f"Failed after {max_retries} attempts")
    return None


# Usage
data = fetch_with_error_handling('https://api.github.com/repos/python/cpython')
if data:
    print(f"Repo: {data['full_name']}, Stars: {data['stargazers_count']}")


# **Expected Log Output:**
# ```
# 2024-03-15 10:30:45 - __main__ - INFO - Attempt 1/3: GET https://api.github.com/...
# 2024-03-15 10:30:46 - __main__ - INFO - ✓ Success: https://api.github.com/...
# ```

# ---
# ## 2.7 Building a Reusable API Client
# 
# Combining everything we've learned into a **clean, professional `GitHubAPI` class** that can be reused across projects.

# In[11]:


class GitHubAPI:
    """
    Reusable GitHub API client with all best practices:
    - Session management with retry logic
    - Rate limiting
    - Authentication via token
    - Logging
    """

    def __init__(self, token=None):
        self.base_url = 'https://api.github.com'
        self.session = self._create_session()  # Robust session with retries
        self.rate_limiter = RateLimiter(
            max_requests=5000, time_window=3600
        )  # Authenticated limits

        # Add authentication token if provided
        if token:
            self.session.headers.update({'Authorization': f'Bearer {token}'})

        # Always set these headers for proper API communication
        self.session.headers.update(
            {
                'Accept': 'application/vnd.github.v3+json',
                'User-Agent': 'Library-Tutorial/1.0',
            }
        )

        self.logger = logging.getLogger(self.__class__.__name__)

    def _create_session(self):
        """Create session with retry logic (private method)."""
        session = requests.Session()
        retry_strategy = Retry(
            total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def get(self, endpoint, params=None):
        """Make GET request with rate limiting."""
        self.rate_limiter.wait_if_needed()  # Respect rate limits
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()  # Raises exception for 4xx/5xx
            self.logger.info(f"GET {endpoint} - Status: {response.status_code}")

            # Peek at remaining rate limit with each response
            remaining = check_rate_limit(response)
            return response.json()

        except Exception as e:
            self.logger.error(f"Error fetching {endpoint}: {e}")
            raise

    def get_repo(self, owner, repo):
        """Get repository information."""
        return self.get(f'/repos/{owner}/{repo}')

    def get_user_repos(self, username):
        """Get all repositories for a user."""
        return self.get(f'/users/{username}/repos', params={'per_page': 100})

    def search_repos(self, query, language=None, min_stars=None):
        """
        Search repositories.

        Args:
            query: Search query string
            language: Filter by programming language
            min_stars: Minimum stars required

        Returns:
            list: Repository results
        """
        # Build search query by combining filters with spaces
        q_parts = [query]
        if language:
            q_parts.append(f"language:{language}")
        if min_stars:
            q_parts.append(f"stars:>={min_stars}")

        q = ' '.join(q_parts)
        results = self.get('/search/repositories', params={'q': q})
        return results['items']

    def to_dataframe(self, repos):
        """Convert repository list to DataFrame for analysis."""
        data = []
        for repo in repos:
            data.append(
                {
                    'name': repo['name'],
                    'full_name': repo['full_name'],
                    'description': repo.get('description'),
                    'stars': repo['stargazers_count'],
                    'forks': repo['forks_count'],
                    'language': repo.get('language'),
                    'created_at': repo['created_at'],
                    'updated_at': repo['updated_at'],
                }
            )
        return pd.DataFrame(data)


# ============================
# Usage Examples
# ============================

api = GitHubAPI(token=os.getenv('GITHUB_TOKEN'))  # Authenticated (5000 req/hr)

# Get a single repository
repo = api.get_repo('pandas-dev', 'pandas')
print(f"Stars: {repo['stargazers_count']}")

# Search repositories with filters
python_repos = api.search_repos('machine learning', language='python', min_stars=1000)
df = api.to_dataframe(python_repos)
print(df.head())


# ---
# ## 2.8 Working with Different Response Formats
# 
# APIs don't always respond with JSON — here's how to handle the two most common formats.
# 
# ---
# 
# ### <span style="color:blue">JSON (Most Common)</span>

# In[12]:


# Simple JSON - direct parsing
response = requests.get("https://api.github.com/repos/pandas-dev/pandas")
data = response.json()

# Nested JSON - requires flattening for DataFrame use
data = {
    "user": {
        "name": "Ahmed",
        "address": {"city": "Cairo", "country": "Egypt"},
        "repositories": [
            {"name": "repo1", "stars": 10},
            {"name": "repo2", "stars": 25},
        ],
    }
}

# Flatten nested JSON with json_normalize - great for deeply nested responses
df = pd.json_normalize(
    data["user"]["repositories"], sep="_"  # Use underscore to separate nested keys
)
print("Simple flatten:")
print(df)

# Or access nested data with record_path and meta
# record_path = which nested list to expand as rows
# meta = which parent fields to carry along as columns
df = pd.json_normalize(
    data,
    record_path=["user", "repositories"],
    meta=[["user", "name"], ["user", "address", "city"]],
    meta_prefix="user_",
)
print("\nWith metadata:")
print(df)


# ### <span style="color:blue">XML</span>
# 
# Some older APIs (e.g., government data, RSS feeds) return XML instead of JSON.

# In[13]:


import xml.etree.ElementTree as ET

# Sample XML response from an API
xml_string = """
<library>
  <book id="1">
    <title>Python Basics</title>
    <author>John Doe</author>
    <year>2023</year>
  </book>
  <book id="2">
    <title>Data Science</title>
    <author>Jane Smith</author>
    <year>2024</year>
  </book>
</library>
"""

# Parse the XML string into a tree structure
root = ET.fromstring(xml_string)

# Navigate the tree and extract data into a list of dicts
books = []
for book in root.findall(
    ".//book"
):  # './/book' finds all <book> tags anywhere in the tree
    books.append(
        {
            "id": book.get("id"),  # Get XML attribute
            "title": book.find("title").text,  # Get text content of child tag
            "author": book.find("author").text,
            "year": int(book.find("year").text),
        }
    )

# Convert to DataFrame for easy analysis
df = pd.DataFrame(books)
print(df)


# ---
# ## <span style="color:blue">2.9 Graded Exercise 2: GitHub Repository Analysis</span>
# 
# **Scenario**: Analyze GitHub repositories to understand popular technologies.
# 
# ---
# 
# ### 📋 Task 1: Repository Information *(15 points)*
# 
# **1.1** *(5 points)* Fetch information for these repositories:
# - `tensorflow/tensorflow`
# - `pytorch/pytorch`
# - `scikit-learn/scikit-learn`
# 
# Create a **DataFrame** with columns: `name`, `stars`, `forks`, `language`, `open_issues`, `created_date`
# 
# Save as: **`task1_github.csv`**

# In[14]:


import matplotlib.pyplot as plt


# In[15]:


def task1_fetch_repos():
    """
    Fetch repository information for major ML frameworks.
    Returns a DataFrame with key metrics.
    """
    repos = ["tensorflow/tensorflow", "pytorch/pytorch", "scikit-learn/scikit-learn"]

    # Your code here
    api = GitHubAPI(token=os.getenv("GITHUB_TOKEN"))
    repos_data = []
    for i in range(len(repos)):
        owner, repo_name = repos[i].split("/")
        repo = api.get_repo(owner, repo_name)
        repo_data = {
            "name": repo["name"],
            "stars": repo["stargazers_count"],
            "forks": repo["forks_count"],
            "language": repo["language"],
            "open_issues": repo["open_issues_count"],
            "created_date": repo["created_at"],
        }
        repos_data.append(repo_data)
    df = pd.DataFrame(repos_data)
    return df


# Call the function
df = task1_fetch_repos()
df.to_csv("task1_github.csv", index=False)


# In[16]:


metric_df = df[["name"]]
metric_df["Age in days"] = (
    pd.to_datetime("today", utc=True) - pd.to_datetime(df["created_date"], utc=True)
).dt.days
metric_df["Stars per day"] = df["stars"] / metric_df["Age in days"]
metric_df["Issues per star ratio"] = df["open_issues"] / df["stars"]
metric_df.to_csv("task1_metrics.csv", index=False)
print(metric_df)


# In[18]:


plt.style.use("dark_background")
fig, ax = plt.subplots(3, 1, figsize=(10, 15))

# Figure title for the entire figure (all subplots)
fig.suptitle("GitHub Repository Metrics Comparison", fontsize=16)

# Subplot 1: Age in days
ax[0].bar(metric_df["name"], metric_df["Age in days"])
ax[0].set_ylabel("Age in days")
ax[0].set_xlabel("Repository")

# Subplot 2: Stars per day
ax[1].bar(metric_df["name"], metric_df["Stars per day"])
ax[1].set_ylabel("Stars per day")
ax[1].set_xlabel("Repository")

# Subplot 3: Issues per star ratio
ax[2].bar(metric_df["name"], metric_df["Issues per star ratio"])
ax[2].set_ylabel("Issues per star ratio")
ax[2].set_xlabel("Repository")

# Adjust layout to prevent overlap
plt.tight_layout(rect=[0, 0.03, 1, 0.95])  # Leave space for suptitle
plt.savefig("task1_comparison.png")
plt.close()


# **1.2** *(5 points)* For each repo, calculate:
# - **Age in days** (from `created_date` to now)
# - **Stars per day**
# - **Issues per star ratio**
# 
# Save as: **`task1_metrics.csv`**
# 
# **1.3** *(5 points)* Create a **visualization** comparing the three repositories:
# - Save as: **`task1_comparison.png`**
# - Use `matplotlib` or `seaborn`
# - Compare **at least 3 metrics**
# 
# ---
# 
# ### 📋 Task 2: User Repository Analysis *(20 points)*
# 
# **2.1** *(10 points)* Choose any GitHub user and fetch **ALL** their repositories (handle pagination)
# 
# Requirements:
# - Implement **pagination** properly
# - Add **rate limiting** (wait 1 second between requests)
# - Handle errors gracefully
# - Log progress
# 
# Save as: **`task2_all_repos.csv`**

# In[ ]:


def fetch_user_repos_paginated(username):
    """
    Fetch all repositories for a user with pagination.

    Args:
        username: GitHub username

    Returns:
        list: All repositories
    """
    all_repos = []
    page = 1

    # Your implementation here
    # Remember to:
    # 1. Check for empty responses  --> means you've hit the last page
    # 2. Add delays                 --> time.sleep(1) to be polite
    # 3. Handle errors              --> try/except around requests
    # 4. Log progress               --> print or logger.info(f"Page {page}")
    all_repos = []
    page = 1
    url = f"https://api.github.com/users/{username}/repos"
    token = os.getenv("GITHUB_TOKEN")
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "Library-Tutorial-App",
    }
    logger = logging.getLogger("fetch_user_repos_paginated")
    while True:
        try:
            logger.info(f"Fetching page {page} for user {username}...")
            params = {"page": page, "per_page": 100}
            response = requests.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()  # Raises HTTPError for bad responses (4xx/5xx)
            data = response.json()

            if not data:
                logger.info("No more repositories found. Ending pagination.")
                break

            all_repos.extend(data)

            if len(data) < 100:
                logger.info("Last page reached with fewer than 100 repositories.")
                break

            logger.info(
                f"Page {page} fetched successfully with {len(data)} repositories."
            )
            page += 1
            time.sleep(1)
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching page {page}: {e}")
            break

    return all_repos


all_repos = fetch_user_repos_paginated("abdulrahmannmostafa")
all_repos_df = pd.DataFrame(all_repos)
all_repos_df.to_csv("task2_all_repos.csv", index=False)


# In[ ]:


most_use_language = all_repos_df["language"].mode()[0]
average_stars = all_repos_df["stargazers_count"].mean()
total_forks = all_repos_df["forks_count"].sum()
most_recent_update_repo = all_repos_df.sort_values("updated_at", ascending=False).iloc[
    0
][
    [
        "name",
        "forks_count",
        "stargazers_count",
        "language",
        "open_issues_count",
        "created_at",
    ]
]
oldest_repo = all_repos_df.sort_values("created_at").iloc[0][
    [
        "name",
        "forks_count",
        "stargazers_count",
        "language",
        "open_issues_count",
        "created_at",
    ]
]
with open("task2_analysis.txt", "w") as f:
    f.write("-------------------------Task 2 Analysis Report----------------------\n")
    f.write(f"Most used programming language across all repos: {most_use_language}\n")
    f.write(f"Average stars per repository: {average_stars}\n")
    f.write(f"Total forks across all repos: {total_forks}\n")
    f.write(f"Most recently updated repo:\n{most_recent_update_repo}\n\n\n")
    f.write(f"Oldest repo:\n{oldest_repo}\n")


# **2.2** *(10 points)* Analyze the repositories:
# - Most used **programming language**
# - **Average stars** per repository
# - **Total forks** across all repos
# - **Most recently updated** repo
# - **Oldest** repo
# 
# Create a summary report saved as: **`task2_analysis.txt`**
# 
# ---
# 
# ### 📋 Task 3: Advanced API Client *(15 points)*
# 
# **3.1** *(15 points)* Build a complete `GitHubAnalyzer` class
# 
# Requirements:
# - **Inherit from** or include rate limiting
# - Implement **retry logic** with exponential backoff
# - Add **logging**
# - Include these methods:
#   - `search_repos(query, language, min_stars)` — Search repositories
#   - `get_trending(language, since)` — Get trending repos
#   - `compare_repos(repo_list)` — Compare multiple repos
#   - `export_to_excel(df, filename)` — Export with formatting

# In[21]:


from openpyxl.styles import Font


class GitHubAnalyzer:
    """
    Complete GitHub API client with analysis capabilities.
    Build on top of the GitHubAPI class concepts from section 2.7.
    """

    def __init__(self, token=None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.session = self._create_session()
        self.rate_limiter = RateLimiter(max_requests=5000, time_window=3600)
        self.base_url = "https://api.github.com"

        if token:
            self.session.headers.update({"Authorization": f"Bearer {token}"})

        self.session.headers.update(
            {
                "Accept": "application/vnd.github.v3+json",
                "User-Agent": "Library-Tutorial",
            }
        )

    def _create_session(self):
        session = requests.Session()
        retry_strategy = Retry(
            total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def get(self, endpoint, params=None):
        self.rate_limiter.wait_if_needed()
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            self.logger.info(f"GET {endpoint} - Status: {response.status_code}")
            check_rate_limit(response)
            return response.json()
        except Exception as e:
            self.logger.error(f"Error fetching {endpoint}: {e}")
            raise

    def get_repo(self, owner, repo):
        return self.get(f"/repos/{owner}/{repo}")

    def to_dataframe(self, repos):
        data = []
        for repo in repos:
            data.append(
                {
                    "name": repo["name"],
                    "full_name": repo["full_name"],
                    "description": repo.get("description"),
                    "stars": repo["stargazers_count"],
                    "forks": repo["forks_count"],
                    "language": repo.get("language"),
                    "created_at": repo["created_at"],
                    "updated_at": repo["updated_at"],
                }
            )
        return pd.DataFrame(data)

    def search_repos(self, query, language=None, min_stars=0):
        """
        Search repositories with filters.

        Returns:
            DataFrame with results
        """
        self.logger.info(
            f"Searching repositories with query: '{query}', language: '{language}', min_stars: {min_stars}"
        )

        query_parts = [query]
        if language:
            query_parts.append(f"language:{language}")
        if min_stars:
            query_parts.append(f"stars:>={min_stars}")

        q = " ".join(query_parts)

        self.logger.info(f"Searching repositories with query: {q}")

        results = self.get("/search/repositories", params={"q": q})
        df = self.to_dataframe(results["items"])

        self.logger.info(f"Found {len(df)} repositories matching criteria.")

        return df

    def compare_repos(self, repo_list):
        """
        Compare multiple repositories.

        Args:
            repo_list: List of "owner/repo" strings

        Returns:
            DataFrame with comparison
        """
        self.logger.info(f"Comparing repositories: {repo_list}")

        repos_data = []
        for repo in repo_list:
            owner, repo_name = repo.split("/")
            repo_info = self.get_repo(owner, repo_name)
            repos_data.append(repo_info)

        df = self.to_dataframe(repos_data)

        self.logger.info(f"Comparison complete. Found {len(df)} repositories.")
        return df

    def export_to_excel(self, df, filename):
        """
        Export DataFrame to Excel with formatting.
        - Bold headers
        - Auto-adjust column widths
        - Add creation timestamp
        """
        if df is None or df.empty:
            self.logger.warning("No data to export.")
            return

        self.logger.info(f"Exporting DataFrame to {filename}")

        with pd.ExcelWriter(
            filename, engine="openpyxl"
        ) as writer:  # Use openpyxl for formatting capabilities
            df.to_excel(writer, index=False, sheet_name="results_sheet")
            results_sheet = writer.sheets[
                "results_sheet"
            ]  # Get the worksheet object to apply formatting

            for cell in results_sheet[1]:
                cell.font = Font(bold=True)
            for column_cells in results_sheet.columns:
                max_length = max(
                    len(str(cell.value)) if cell.value is not None else 0
                    for cell in column_cells
                )  # Find the longest cell in the column and set the width accordingly
                width = max_length + 3  # Add some extra space for better readability
                results_sheet.column_dimensions[column_cells[0].column_letter].width = (
                    width  # Auto-adjust column width based on content
                )
            results_sheet[f"A{results_sheet.max_row + 2}"].value = (
                f"Created at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )


# Test your class by:
# 1. Searching for "data science" repos in Python with >500 stars
analyzer = GitHubAnalyzer(token=os.getenv("GITHUB_TOKEN"))
search_results = analyzer.search_repos("data science", language="python", min_stars=500)
print(search_results.head())

# 2. Comparing 5 repos of your choice
repos_to_compare = [
    "pandas-dev/pandas",
    "scikit-learn/scikit-learn",
    "tensorflow/tensorflow",
    "pytorch/pytorch",
    "keras-team/keras",
]
comparison_df = analyzer.compare_repos(repos_to_compare)
print(comparison_df)

# 3. Exporting results to task3_results.xlsx
analyzer.export_to_excel(comparison_df, "task3_results.xlsx")


# ---
# ## 📦 Submission Requirements
# 
# ### Files to submit:
# 
# | # | File | Description |
# |---|---|---|
# | 1 | `github_analysis.py` | All your code |
# | 2 | `task1_github.csv` | Repo info table |
# | 3 | `task1_metrics.csv` | Calculated metrics |
# | 4 | `task1_comparison.png` | Visualization |
# | 5 | `task2_all_repos.csv` | All user repos |
# | 6 | `task2_analysis.txt` | Summary report |
# | 7 | `task3_results.xlsx` | Excel export |
# | 8 | `api_requests.log` | Your log file |
# | 9 | `README.md` | Brief report of findings |
# 
# ---
# 
# ### 📊 Grading Rubric:
# 
# | Category | Weight |
# |---|---|
# | **Correct functionality** | 60% |
# | **Code quality** (comments, error handling, logging) | 20% |
# | **Output formatting** | 10% |
# | **Analysis insights** | 10% |
