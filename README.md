# DataEngineering-ChangeCaptureWithPandas-CodeSamples


## Table of Contents

* [1. About the Project](#1-about-the-project)
* [2. Requirements](#2-requirements)
* [3. Getting Started](#3-getting-started)
* [4. Basic Usage](#4-basic-usage)
* [5. License](#5-license)

## 1. About the project

This is a demonstrative implementation for CDC (Change Data Capture) in data integration. The main idea is to provide a module able to compare two successive states of a table (coming from a .csv file, for example) and determine what was changed from the previous state to the current one. Basically, the changes that can be identified are when cells in the table was created, updated or deleted.

CDC is a method that has been used to watch what is happening in data sources willing to promote data integration among multiples storage technologies in the context of a data architecture, widely used both for batch and stream data processing. Modern databases make available the possibility of using triggers to flag up its changes and even external complex stuff like Debezium have implemented in a more generic scope. However this implementation aims to be a simple demonstration of how works and can be used a limited scope or as a temporary building block for wider implementations.

#### limitations

* amount of data
* running time (depends on scheduling)

#### Built with

* Python 3.9
* Pandas

## 2. Requirements

* Python 3.9 installed

## 3. Getting Started

#### Cloning this project and installing its dependencies

&nbsp;&nbsp;1. Clone repository:

```
git clone https://github.com/smurilogs/DataEngineering-ChangeCaptureWithPandas-CodeSamples.git
```
&nbsp;&nbsp;2. Install dependencies:

```
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## 4. Basic Usage

The inputs for this module is always two Pandas dataframes (last and current states for the same table). The project assumes that a copy of the last capture should be always in storage so that it can be used for comparations to capture the changes. Both last and current state dataframes can come from any sources types supported by Pandas. An important detail on this project is that all columns of the data frame must be forced to be string type. It's basically to keep it simple.

The user must specify a set of columns as a compose key for the dataframe. It's important because the module is going to use these definitions to make the comparations. So, apart form this set, all other columns will be subject for comparations and change captures.

The captures are going to be displayed data frames that can be invoked by different methods, each of them making difference by type of change (`creation`, `update` or `delete`) and by how it will be assembled (`wide` or `long`). Each method labels the table with its captures with the data source identification, type of change and change timestamp.

Example of usage:



## 5. License

Distributed under the MIT License. See `LICENSE` for more information.