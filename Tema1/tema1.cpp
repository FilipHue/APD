#include <iostream>
#include <functional>
#include <fstream>
#include <unistd.h>
#include <pthread.h>
#include <cmath>
#include <queue>
#include <set>

pthread_mutex_t mutex;
pthread_barrier_t barrier;

typedef struct parameters {
    int numberOfMappers;
    int numberOfReducers;
    int numberOfFiles;
    std::string input_file;
} parameters;

typedef struct thread_info {
    int id;
    int number_of_files;
    int mappers;
    int reducers;
    std::queue<std::string> *files;
    std::vector<std::vector<long long>> *preliminaryResults;
} thread_info;

int mapperFunction(int number, int exponent) {
    long long lowBound, highBound, mid;

    if (number == 1) {
        return 1;
    }

    lowBound = 2;
    highBound = lowBound;

    while (pow(highBound, exponent) < number) {
        highBound *= 2;
    }

    while (highBound - lowBound > 1) {
        mid = (lowBound + highBound) / 2;
        
        if (pow(mid, exponent) <= number) {
            lowBound = mid;
        } else {
            highBound = mid;
        }
    }

    if (pow(lowBound, exponent) == number) {
        return number;
    }

    return -1;
}

void reducerFunction() {

}

void *threadFunc(void *args) {
    thread_info *info;
    std::queue<std::string> *files;
    std::vector<std::vector<long long>> *preliminaryResults;
    int id;

    info = (thread_info *)args;
    id = info->id;
    files = info->files;
    preliminaryResults = info->preliminaryResults;

    if (id < info->mappers) {
        while (!files->empty()) {
            std::ifstream inputFile;
            std::string fileName;
            std::vector<int> numbers;
            int numberOfSamples;

            pthread_mutex_lock(&mutex);
            fileName = files->front();
            files->pop();
            pthread_mutex_unlock(&mutex);

            if (fileName.empty()) {
                return (int *)-1;
            }

            // std::cout << fileName << std::endl;

            inputFile.open(fileName);
            if (inputFile.is_open()) {

                inputFile >> numberOfSamples;
                for (int i = 0; i < numberOfSamples; i++) {
                    int number;

                    inputFile >> number;
                    numbers.push_back(number);
                }
            }
            inputFile.close();
            // std::cout << "Thread " << id << " read from file " << fileName << " " << numbers.size() << " numbers\n";

            preliminaryResults->resize(info->reducers);
            for (int e = 0; e < info->reducers; e++) {
                for (int i = 0; i < numberOfSamples; i++) {
                    int number;

                    number = mapperFunction(numbers[i], e + 2);
                    if (number != -1) {
                        preliminaryResults->at(e).push_back(number);
                    }
                }
            }

            std::cout << "\nThread " << id << " processed:\n\t";
            for (int i = 0; i < info->reducers; i++) {
                std::cout << "For exponent " << i + 2 << ": ";
                for (int j = 0; j < (int)preliminaryResults->at(i).size(); j++) {
                    std::cout << preliminaryResults->at(i).at(j) << " ";
                }
                std::cout << "\n";
                if (i < info->reducers - 1) {
                    std::cout << "\t";
                }
            }
        }
    }
    pthread_barrier_wait(&barrier);
    if (id >= info->mappers) {
        // std::cout << "Thread " << id << " started...\n";
        std::set<long long> finalResults;
        std::string outputFileName;
        std::ofstream outputFile;

        int index;

        index = abs(id - info->mappers);
        for (int i = 0; i < (int)preliminaryResults->at(index).size(); i++) {
            finalResults.insert(preliminaryResults->at(index)[i]);
        }

        outputFileName = "out" + std::to_string(index + 2) + ".txt";
        outputFile.open(outputFileName);
        outputFile << finalResults.size();
        outputFile.close();
        // std::cout << "Thread " << id << " finished\n";
    }

    pthread_exit(NULL);
}

int main(int argc, char **argv) {
    parameters params;
    std::ifstream inputFile;
    std::string fileName;
    std::queue<std::string> *fileNames = new std::queue<std::string>();
    std::vector<std::vector<long long>> *preliminaryResults = new std::vector<std::vector<long long>>();


    if (argc < 4) {
        std::cout << "Incorrect usage - need 3 arguments:\n\tNumber of mapers\n\tNumber of reducers\n\tInput file\n";
        return -1;
    }

    params.numberOfMappers = atoi(argv[1]);
    params.numberOfReducers = atoi(argv[2]);
    params.input_file = argv[3];

    inputFile.open(params.input_file);
    inputFile >> params.numberOfFiles;
    while (inputFile >> fileName) {
        fileNames->push(fileName);
    }

    int thread;
    pthread_t threads[params.numberOfMappers + params.numberOfReducers];
    thread_info info[params.numberOfMappers + params.numberOfReducers];

    pthread_mutex_init(&mutex, NULL);
    pthread_barrier_init(&barrier, NULL, params.numberOfMappers + params.numberOfReducers);
    for (int i = 0; i < params.numberOfMappers + params.numberOfReducers; i++) {
        info[i].id = i;
        info[i].number_of_files = params.numberOfFiles;
        info[i].mappers = params.numberOfMappers;
        info[i].reducers = params.numberOfReducers;
        info[i].files = fileNames;
        info[i].preliminaryResults = preliminaryResults;


        thread = pthread_create(&threads[i], NULL, threadFunc, &info[i]);
        if (thread) {
	  		printf("Eroare la crearea thread-ului %d\n", i);
	  		exit(-1);
		}
    }


    for (int i = 0; i < params.numberOfMappers + params.numberOfReducers; ++i) {
		thread = pthread_join(threads[i], NULL);
        if (thread) {
	  		printf("Eroare la asteptarea thread-ului %d\n", i);
	  		exit(-1);
		}
    }
    pthread_barrier_destroy(&barrier);
    pthread_mutex_destroy(&mutex);

    delete(fileNames);
    delete(preliminaryResults);

    return 0;
}