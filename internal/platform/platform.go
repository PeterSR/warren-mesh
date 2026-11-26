// Package platform holds the few operating system calls Warren makes that
// differ between systems, behind small functions: peer credentials of a unix
// socket connection, hardening the process against same-user inspection,
// becoming a subreaper, and how long a unix socket path may be. The Linux
// versions are complete; the others say what protection they lose.
package platform
