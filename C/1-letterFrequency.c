#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>
#include <ctype.h>

#define NUM_WORKERS 2
#define CHUNK_SIZE (1024 * 1024)  // 1 MB

typedef struct {
    char *data;
    size_t start;
    size_t end;
    unsigned long counts[26];
} ThreadData;

void *count_letters_in_chunk(void *arg) {
    ThreadData *td = (ThreadData *)arg;
    memset(td->counts, 0, 26 * sizeof(int));

    for (size_t i = td->start; i < td->end; ++i) {
        char c = tolower(td->data[i]);
        if (c >= 'a' && c <= 'z') {
            td->counts[c - 'a']++;
        }
    }
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <file_path>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    const char *file_path = argv[1];

    // Open the file
    int fd = open(file_path, O_RDONLY);
    if (fd == -1) {
        perror("open");
        exit(EXIT_FAILURE);
    }

    // Get the file size
    struct stat sb;
    if (fstat(fd, &sb) == -1) {
        perror("fstat");
        exit(EXIT_FAILURE);
    }
    size_t file_size = sb.st_size;

    // Memory-map the file
    char *data = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        perror("mmap");
        close(fd);
        exit(EXIT_FAILURE);
    }
    close(fd);

    // Create threads
    pthread_t threads[NUM_WORKERS];
    ThreadData thread_data[NUM_WORKERS];
    size_t chunk_size = file_size / NUM_WORKERS;

    for (int i = 0; i < NUM_WORKERS; ++i) {
        thread_data[i].data = data;
        thread_data[i].start = i * chunk_size;
        thread_data[i].end = (i == NUM_WORKERS - 1) ? file_size : (i + 1) * chunk_size;
        pthread_create(&threads[i], NULL, count_letters_in_chunk, &thread_data[i]);
    }

    // Collect results
    unsigned long total_counts[26] = {0};
    unsigned long total = 0;
    for (int i = 0; i < NUM_WORKERS; ++i) {
        pthread_join(threads[i], NULL);
        for (int j = 0; j < 26; ++j) {
            total_counts[j] += thread_data[i].counts[j];
            total += thread_data[i].counts[j];
        }
    }

    // Unmap the file
    munmap(data, file_size);

    // Print the results
    for (char c = 'a'; c <= 'z'; ++c) {
        printf("%c: %f %%\n", c, (100*(double)total_counts[c - 'a'] / total));
    }

    return 0;
}

