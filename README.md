# Project's Title 
FlightsData

# Project Description
The project aims to provide meaningful insights using the data from flights and passengers movements.
The output of the statistics are stored in the `csv` file format.

# Technologies Used
SBT, Scala, Spark (local mode), Funsuite for Unit Testing
Spark technology stack was chosen to derive insights as it provides both batch and streaming capabilities, analyse huge voulme of datasets on a cluster of machines. This is scable, deployed to use Production grade data loads.

# How to Install and Run the Project
This project is built using Scala SBT. Either cloning the repository from GitHub or downloading the ZIP file, you can import to your local.
I have used Intellij IDEA as the editor and would also recommend using for Scala/ Spark projects.

This project follows the standard folder structure and source code are found under `src`.
`main` contains the business or processing logic
`test` contains the unit test files

This is configured to run in the local or client mode setup and if you were to run in the `cluster` mode, you may want to externalise the cluster URL to the SparkSession as `Program Arguments`

# How to Use the Project
Recommend 
1. Read and understand the static data under `data` folder
2. Read the unit tests
3. Read the business logic as each use-case is separated into individual Spark jobs