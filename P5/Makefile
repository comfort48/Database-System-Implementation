CC = g++ -O2 -Wno-deprecated 

tag = -i

ifdef linux
tag = -n
endif

a5.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Heap.o SortedFile.o Pipe.o BigQ.o RelOp.o Statistics.o Function.o QueryTreeNode.o Optimizer.o y.tab.o lex.yy.o main.o 
	$(CC) -o a5.out Record.o Comparison.o ComparisonEngine.o Schema.o Function.o File.o DBFile.o Heap.o SortedFile.o Pipe.o BigQ.o RelOp.o Statistics.o QueryTreeNode.o Optimizer.o y.tab.o lex.yy.o main.o -ll -lpthread
	
gtest: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Heap.o SortedFile.o Pipe.o BigQ.o RelOp.o Statistics.o Function.o QueryTreeNode.o Optimizer.o y.tab.o lex.yy.o gtest.o
	$(CC) -o gtest Record.o Comparison.o ComparisonEngine.o Schema.o Function.o File.o DBFile.o Heap.o SortedFile.o Pipe.o BigQ.o RelOp.o Statistics.o QueryTreeNode.o Optimizer.o y.tab.o lex.yy.o gtest.o -ll -lpthread -lgtest

main.o: main.cc
	$(CC) -g -c main.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc
	
Optimizer.o: Optimizer.cc
	$(CC) -g -c Optimizer.cc

QueryTreeNode.o:QueryTreeNode.cc
	$(CC) -g -c QueryTreeNode.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

Heap.o: Heap.cc
	$(CC) -g -c Heap.cc

SortedFile: SortedFile.cc
	$(CC) -g -c SortedFile.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc
	
Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc
	
BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc
	
RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" y.tab.c
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
