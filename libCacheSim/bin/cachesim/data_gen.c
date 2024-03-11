#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <time.h>
typedef struct oracleGeneral{
    __uint32_t real_time;
    __uint64_t obj_id;
    __uint32_t obj_size;
    __int64_t next_access_vtime;
} oracleGeneral;
// Function to generate the Zipf distribution array
double *generate_zipf_distribution(int n, double alpha) {
    double *arr = (double *)malloc((n + 1) * sizeof(double));
    if (arr == NULL) {
        fprintf(stderr, "Memory allocation failed\n");
        exit(EXIT_FAILURE);
    }

    double z = 0.0;  // Normalization constant
    for (int i = 1; i <= n; i++) {
        z += 1.0 / pow((double)i, alpha);
    }

    arr[0] = 0.0;  // First index is not used
    for (int i = 1; i <= n; i++) {
        arr[i] = arr[i - 1] + (1.0 / pow((double)i, alpha)) / z;
    }

    return arr;
}

// Function to find an index in the Zipf distribution
int sample_zipf(double *zipf_distribution, int n) {
    double u = (double)rand() / RAND_MAX;
    // Linear search
    for (int i = 1; i <= n; i++) {
        if (u <= zipf_distribution[i]) {
            return i;
        }
    }
    return n; // In case of numerical inaccuracies
}

oracleGeneral** generate_file(double alpha, int num_requests, int num_objects, char *output_file_name) {
    srand((unsigned)time(NULL));  // Seed the random number generator

    double *zipf_distribution = generate_zipf_distribution(num_objects, alpha);

    FILE *output_file = fopen(output_file_name, "wb");
    if (output_file == NULL) {
        fprintf(stderr, "Could not open file %s for writing\n", output_file_name);
        exit(EXIT_FAILURE);
    }

    oracleGeneral** res = malloc(num_requests * sizeof(oracleGeneral*));
    for (int i = 0; i < num_requests; i++) {
        int zipf_value = sample_zipf(zipf_distribution, num_objects);
        oracleGeneral* sample = malloc(sizeof(oracleGeneral));
        sample->real_time = 0;
        sample->obj_id = zipf_value;
        sample->obj_size = 1;
        sample->next_access_vtime = -1;
        fwrite(sample, sizeof(oracleGeneral), 1, output_file);
        res[i] = sample;
    }

    // for (int i = 0; i < num_requests; i++) {
    //     int zipf_value = sample_zipf(zipf_distribution, num_objects);
    //     oracleGeneral* sample = malloc(sizeof(oracleGeneral));
    //     sample->real_time = 0;
    //     sample->obj_id = zipf_value;
    //     sample->obj_size = 1;
    //     sample->next_access_vtime = -1;
    //     fwrite(sample, sizeof(oracleGeneral), 1, output_file);
    // }

    fclose(output_file);
    free(zipf_distribution);
    return res;
}

int main(int argc, char *argv[]) {
    if (argc < 5) {
        fprintf(stderr, "Usage: %s <alpha> <num_requests> <num_objects> <output_file>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    double alpha = atof(argv[1]);
    int num_requests = atoi(argv[2]);
    int num_objects = atoi(argv[3]);
    char *output_file_name = argv[4];

    srand((unsigned)time(NULL));  // Seed the random number generator

    double *zipf_distribution = generate_zipf_distribution(num_objects, alpha);

    FILE *output_file = fopen(output_file_name, "wb");
    if (output_file == NULL) {
        fprintf(stderr, "Could not open file %s for writing\n", output_file_name);
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < num_requests; i++) {
        int zipf_value = sample_zipf(zipf_distribution, num_objects);
        oracleGeneral* sample = malloc(sizeof(oracleGeneral));
        sample->real_time = 0;
        sample->obj_id = zipf_value;
        sample->obj_size = 1;
        sample->next_access_vtime = -1;
        fwrite(sample, sizeof(oracleGeneral), 1, output_file);
    }

    fclose(output_file);
    free(zipf_distribution);

    return 0;
}
