# CS4224 Cassandra

## Project Setup

### 1. Install Cassandra

Please refer to the installation instructions from Cassandra docs:
http://cassandra.apache.org/doc/latest/cassandra/getting_started/installing.html

### 2. Install Python dependencies

You may use either pipenv or manual installation to install the python dependencies.

a) Using pipenv
```
pip install pipenv
pipenv install
pipenv shell
```

b) Manual installation
```
pip install cassandra-driver
```

### 3. Download data from CS4224 website

The data files are not included in this repository due to their large size. You may either download and extract them manually, or run the following commands in the project's root.

```bash
wget http://www.comp.nus.edu.sg/~cs4224/project_files_3.zip
unzip project-files.zip
```

Now, you should see a folder called `project_files_3` in the project's root folder.