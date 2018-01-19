#pragma once
#include <string>
#include <vector>
#include <set>

#define ROW_SIZE 6
#define MAX_LENGTH 255

///***	   non-class functions    ***
int API_TimeToStringEX(std::string &strDateStr, const time_t &timeData);
int API_StringToTimeEX(const std::string &strDateStr, time_t &timeData);

// overload for postgreSQL
std::string get_ligra(
	// connection parameters
	std::string host_name,
	std::string port_name,
	std::string db_name,
	std::string schema_name,
	std::string table_name,
	std::string user,
	std::string password,
	// sql paramsters
	std::string src_account,
	std::string dst_account,
	std::string date,
	std::string money,
	std::string record_id,
	std::string direction,
	std::string cond);

// overload for MySQL
std::string get_ligra(
	// connection parameters
	std::string host_name,
	std::string port_name,
	std::string db_name,
	std::string table_name,
	std::string user,
	std::string password,
	// sql paramsters
	std::string src_account,
	std::string dst_account,
	std::string date,
	std::string money,
	std::string record_id,
	std::string direction,
	std::string cond);

///***	  the class definition   ***
struct Node {
	int id;
	int offset;
	std::set<int> dst;
	Node() :id(0), offset(0) {}
	static bool cmp(const Node &a, const Node &b);
};

class DBPreloader {
private:
	wchar_t* Utf8_2_Unicode(char* row_i);
	time_t format_date(std::string date_word);
	std::string format_dquote(std::string field);
	bool null_field(const char (&row)[ROW_SIZE][MAX_LENGTH], int row_size);
public:
	DBPreloader() = default;
	~DBPreloader() = default;
public:
	std::string create_dst_dir(std::string dst_directory);
	std::string rename_file(std::string dst_directory, std::string table_name);
	void csv2ligra(std::string dst_directory);

	// overload for postgreSQL
	void preload_csv(
		std::string dst_directory,
		// connection parameters
		std::string host_name,
		std::string port_name,
		std::string db_name,
		std::string schema_name,
		std::string table_name,
		std::string user,
		std::string password,
		// sql paramsters
		std::string src_account,
		std::string dst_account,
		std::string date,
		std::string money,
		std::string record_id,
		std::string direction,
		std::string cond);

	// overload for MySQL
	void preload_csv(
		std::string dst_directory,
		// connection parameters
		std::string host_name,
		std::string port_name,
		std::string db_name,
		std::string table_name,
		std::string user,
		std::string password,
		// sql paramsters
		std::string src_account,
		std::string dst_account,
		std::string date,
		std::string money,
		std::string record_id,
		std::string direction,
		std::string cond);
};