#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <sys/attr.h>
#include <sys/stat.h>

int main(void) {
    char path[] = "/private/tmp/serapendiya-rename-probe-XXXXXX";
    if (!mkdtemp(path)) { perror("mkdtemp"); return 1; }
    int dir = open(path, O_RDONLY | O_DIRECTORY);
    if (dir < 0) { perror("open directory"); return 1; }
    int a = openat(dir, "a", O_WRONLY | O_CREAT | O_EXCL, 0600);
    int b = openat(dir, "b", O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (a < 0 || b < 0 || write(a, "A", 1) != 1 || write(b, "B", 1) != 1 || fsync(a) || fsync(b)) { perror("setup"); return 1; }
    close(a); close(b);
    errno = 0;
    if (renameatx_np(dir, "a", dir, "b", RENAME_EXCL) != -1 || errno != EEXIST) { perror("existing destination"); return 1; }
    char c;
    b = openat(dir, "b", O_RDONLY);
    if (b < 0 || read(b, &c, 1) != 1 || c != 'B') return 1;
    close(b);
    if (renameatx_np(dir, "a", dir, "c", RENAME_EXCL)) { perror("exclusive rename"); return 1; }
    if (fsync(dir)) { perror("directory fsync"); return 1; }
    printf("exclusive rename preserved existing destination and renamed to absent name; directory fsync returned success; path=%s\n", path);
    close(dir);
    return 0;
}
