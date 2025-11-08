# MapReduce Word Count with RPyC and Docker

## Setup
1. Clone the repository:
   git clone https://github.com/Sinister-999/CSE239-HW2
   cd CSE239-HW2

2. Build the Docker images:
   docker-compose build

3. Run the containers:
   docker-compose up

The coordinator will:
- Download the dataset (enwik9.zip)
- Split the file into chunks
- Distribute work among workers
- Aggregate results and print the 20 most frequent words

## Scaling
Modify the number of worker services in `docker-compose.yml` to test performance with 1, 2, 4, and 8 workers.

## Dataset
The dataset is downloaded from:
https://mattmahoney.net/dc/enwik8.zip

