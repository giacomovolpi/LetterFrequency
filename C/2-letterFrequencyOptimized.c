#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>
#include <ctype.h>
#include <limits.h>

#define NUM_WORKERS 4
#define CHUNK_SIZE (256 * 1024 * 1024)  // 1 MB
#define WORKER_CHUNK_SIZE (CHUNK_SIZE / NUM_WORKERS)

typedef struct {
    unsigned char *data;
    unsigned long start;
    unsigned long counts[UCHAR_MAX];
} ThreadData;

void *count_letters_in_chunk(void *arg) {
    ThreadData *td = (ThreadData *)arg;
    memset(td->counts, 0, UCHAR_MAX * sizeof(unsigned long));

    for (size_t i = 0; i < WORKER_CHUNK_SIZE; ++i) {
        td->counts[td->data[i + td->start]]++;
    }
    return NULL;
}

size_t align_to_chunk(size_t size) {
    return ((size + CHUNK_SIZE - 1) / CHUNK_SIZE) * CHUNK_SIZE;
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <file_path>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    const char *file_path = argv[1];

    // Open the file
    int fd = open(file_path, O_RDWR);
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
    size_t file_size_aligned = align_to_chunk(file_size);

    if (file_size_aligned != file_size) {
        if (ftruncate(fd, file_size_aligned) == -1) {
            perror("ftruncate");
            exit(EXIT_FAILURE);
        }
    }

    // Memory-map the file
    unsigned char *data = mmap(NULL, file_size_aligned, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        perror("mmap");
        close(fd);
        exit(EXIT_FAILURE);
    }

    // Zero padding the extended part of the file, if any
    if (file_size_aligned > file_size) {
        memset((unsigned char *)data + file_size, 0, file_size_aligned - file_size);
    }

    // Create threads
    pthread_t threads[NUM_WORKERS];
    ThreadData thread_data[NUM_WORKERS];

    unsigned long total_counts[26] = {0};
    unsigned long total = 0;
    for (size_t j = 0; j < file_size_aligned; j += CHUNK_SIZE) {
        for (int i = 0; i < NUM_WORKERS; ++i) {
            thread_data[i].data = data;
            thread_data[i].start = j + (i * WORKER_CHUNK_SIZE);
            pthread_create(&threads[i], NULL, count_letters_in_chunk, &thread_data[i]);
        }

        // Collect results
        for (int i = 0; i < NUM_WORKERS; ++i) {
            pthread_join(threads[i], NULL);
            for (unsigned char j = 0; j < CHAR_MAX; ++j) { // only check valid ascii chars
                if (j >= 'a' && j <= 'z'){
                    total_counts[j-'a'] += thread_data[i].counts[j];
                    total += thread_data[i].counts[j];
                } else if (j >= 'A' && j <= 'Z') {
                    total_counts[j-'A'] += thread_data[i].counts[j];
                    total += thread_data[i].counts[j];
                }
            }
        }

    }

    // Unmap the file
    if (munmap(data, file_size_aligned) == -1) {
        perror("munmap");
        exit(EXIT_FAILURE);
    }

    if (ftruncate(fd, file_size) == -1) {
        perror("ftruncate");
        exit(EXIT_FAILURE);
    }

    close(fd);

    // Print the results
    for (char c = 'a'; c <= 'z'; ++c) {
        printf("%c: %f %%\n", c, 100*((float)total_counts[c - 'a'] / total));
    }

    return 0;
}

