
.PHONY: all
all: mysock.so

mysock.so: mysock.c
	gcc -shared -fPIC $^ -o $@ -lluajit-5.1 -lcrypto
