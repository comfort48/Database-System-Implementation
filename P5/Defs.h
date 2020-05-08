#ifndef DEFS_H
#define DEFS_H


#define MAX_ANDS 20
#define MAX_ORS 20

#define MAX_PROJ

#define PAGE_SIZE 131072

//#define catalog_path  "catalog"
//#define dbfile_dir  "../DBI_Data/1G/bin/"
//#define tpch_dir "../DBI_Data/1G/"


#define output_path "output_path"

//#define RUNLEN 500
#define PIPE_SIZE 100

enum Target {Left, Right, Literal};
enum CompOperator {LessThan, GreaterThan, Equals};
enum Type {Int, Double, String};

enum QueryNodeType {SELECTFILE, SELECTPIPE, PROJECT, JOIN, SUM, GROUP_BY, DISTINCT, WRITEOUT};

//unsigned int Random_Generate();


#endif

extern char catalog_path[50];
extern char dbfile_dir[50];
extern char tpch_dir[50];
extern int RUNLEN;
