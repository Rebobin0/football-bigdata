# Football Big Data Project

Big Data project focused on football analytics using Apache Spark, AWS S3, MongoDB Atlas, and machine learning models.

## Technologies

- Python 3.12
- Apache Spark 3.5.8
- Hadoop 3.3.4
- AWS S3
- MongoDB Atlas
- PySpark
- WSL2 (recommended)

---

# Project Structure

```bash
football-bigdata/
│
├── src/
│   └── spark_jobs/
│
├── notebooks/
├── data/
├── requirements.txt
├── .env.example
└── README.md
```

---

# Requirements

Before starting, install:

- Java 17
- Python 3.12
- Apache Spark 3.5.8
- AWS CLI
- Git

---

# Verify Installations

```bash
java -version
python3 --version
spark-submit --version
aws --version
git --version
```

---

# AWS Credentials

Configure AWS Academy credentials locally.

Create:

```bash
~/.aws/credentials
```

and:

```bash
~/.aws/config
```

The repository does NOT store AWS secrets.

---

# Clone Repository

```bash
git clone <repo_url>
cd football-bigdata
```

---

# Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate
```

---

# Install Dependencies

```bash
pip install -r requirements.txt
```

---

# Configure Environment Variables

Create:

```bash
.env
```

based on:

```bash
.env.example
```

---

# Configure Spark Variables

```bash
source setup.sh
```

---

# Run Spark Test

```bash
python src/spark_jobs/test_s3.py
```